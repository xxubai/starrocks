// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/variant_functions.h"

#include <memory>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "jsonpath.h"
#include "runtime/runtime_state.h"
#include "util/variant_converter.h"
#include "variant_path_parser.h"

namespace starrocks {

StatusOr<ColumnPtr> VariantFunctions::variant_query(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns.size() != 2) {
        return Status::InvalidArgument("VariantFunctions::variant_query requires 2 arguments");
    }

    return _do_variant_query<TYPE_VARIANT>(context, columns);
}

Status VariantFunctions::preload_variant_segments(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Check if the json path column is constant
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    const auto path_col = context->get_constant_column(1);
    const Slice variant_path = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_col);

    std::string path_string = variant_path.to_string();
    auto variant_path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!variant_path_status.ok(), variant_path_status.status());
    auto* path_state = new NativeVariantPath();
    path_state->variant_path.reset(std::move(variant_path_status.value()));
    context->set_function_state(scope, path_state);

    return Status::OK();
}

static StatusOr<VariantPath*> get_or_parse_variant_segments(FunctionContext* context, const Slice path_slice,
                                                            VariantPath* variant_path) {
    auto* cached = reinterpret_cast<NativeVariantPath*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (cached != nullptr) {
        // If we already have parsed segments, return them
        return &cached->variant_path;
    }

    std::string path_string = path_slice.to_string();
    auto path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!path_status.ok(), path_status.status());
    variant_path->reset(std::move(path_status.value()));

    return variant_path;
}

Status VariantFunctions::clear_variant_segments(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* variant_segments =
                reinterpret_cast<std::vector<VariantPathExtraction>*>(context->get_function_state(scope));
        if (variant_segments != nullptr) {
            delete variant_segments;
        }
    }

    return Status::OK();
}

template <LogicalType ResultType>
StatusOr<ColumnPtr> VariantFunctions::_do_variant_query(FunctionContext* context, const Columns& columns) {
    size_t num_rows = columns[0]->size();

    auto variant_viewer = ColumnViewer<TYPE_VARIANT>(columns[0]);
    auto json_path_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    ColumnBuilder<ResultType> result(num_rows);

    VariantPath stored_path;
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row) || json_path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto path_slice = json_path_viewer.value(row);
        auto variant_segments_status = get_or_parse_variant_segments(context, path_slice, &stored_path);
        if (!variant_segments_status.ok()) {
            result.append_null();
            continue;
        }

        auto variant_value = variant_viewer.value(row);

        if (!variant_value) {
            result.append_null();
            continue;
        }

        auto metadata = variant_value->get_metadata();
        auto value = variant_value->get_value();

        if (metadata.empty() && value.empty()) {
            result.append_null();
            continue;
        }

        try {
            Variant variant(metadata, value);
            auto variant_result = VariantPath::seek(&variant, variant_segments_status.value());
            if (!variant_result.ok()) {
                result.append_null();
                continue;
            }

            std::cout << "Processing row " << row << ": " << variant_value->to_string() << std::endl;
            std::cout << "Using path: " << path_slice.to_string() << std::endl;
            std::cout << "Variant query result: " << variant_result.value().to_value()->to_string() << std::endl;
            RuntimeState* state = context->state();
            cctz::time_zone zone;
            if (state == nullptr) {
                zone = cctz::local_time_zone();
            } else {
                zone = context->state()->timezone_obj();
            }

            std::cout << "About to call cast_variant_value_to..." << std::endl;

            if constexpr (ResultType == TYPE_VARIANT) {
                // For TYPE_VARIANT, convert back to VariantValue
                auto variant_value_result = variant_result.value().to_value();
                if (!variant_value_result.ok()) {
                    result.append_null();
                    continue;
                }

                // Now VariantValue owns its data, so this is safe
                std::cout << "Converting variant to VariantValue: " << variant_value_result->to_string() << std::endl;
                result.append(std::move(variant_value_result.value()));
                std::cout << "cast_variant_value_to succeeded" << std::endl;
            } else {
                Status status = cast_variant_value_to<ResultType, true>(variant_result.value(), zone, result);
                std::cout << "cast_variant_value_to returned with status: " << status.to_string() << std::endl;
                if (!status.ok()) {
                    std::cout << "Cast variant value failed: " << status.to_string() << std::endl;
                    result.append_null();
                } else {
                    std::cout << "cast_variant_value_to succeeded" << std::endl;
                }
            }
        } catch (const std::exception& e) {
            result.append_null();
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks
