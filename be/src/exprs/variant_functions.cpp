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

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "jsonpath.h"
#include "runtime/runtime_state.h"
#include "variant_path_parser.h"
#include <unordered_map>
#include <memory>

namespace starrocks {

StatusOr<ColumnPtr> VariantFunctions::variant_query(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns.size() != 2) {
        return Status::InvalidArgument("VariantFunctions::variant_query requires 2 arguments");
    }

    return _do_variant_query<TYPE_VARIANT>(context, columns);
}

Status VariantFunctions::preload_variant_segments(FunctionContext* context, FunctionContext::FunctionStateScope scope){
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Check if the json path column is constant
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    const auto path_col = context->get_constant_column(1);
    const Slice variant_path = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_col);
    VariantPathParser parser(variant_path.to_string());
    auto variant_segments = parser.parse();
    RETURN_IF(!variant_segments.ok(), variant_segments.status());
    context->set_function_state(scope, &variant_segments.value());
    VLOG(10) << "Preloaded variant segments: " << variant_path.to_string();

    return Status::OK();
}

static StatusOr<std::vector<VariantPathSegmentPtr>*> get_or_parse_variant_segments(FunctionContext* context, const Slice variant_path) {
    auto* variant_segments = reinterpret_cast<std::vector<VariantPathSegmentPtr>*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (variant_segments != nullptr) {
        // If we already have parsed segments, return them
        return variant_segments;
    } else {
        if (context->is_notnull_constant_column(1)) {
            VariantPathParser parser(variant_path.to_string());
            auto segments = parser.parse();
            RETURN_IF(!segments.ok(), segments.status());
            variant_segments = new std::vector(std::move(segments.value()));
            context->set_function_state(FunctionContext::FRAGMENT_LOCAL, variant_segments);
            return variant_segments;
        } else {
            static thread_local std::unordered_map<std::string, std::unique_ptr<std::vector<VariantPathSegmentPtr>>> path_cache;

            std::string path_key = variant_path.to_string();
            auto it = path_cache.find(path_key);
            if (it != path_cache.end()) {
                return it->second.get();
            }

            VariantPathParser parser(path_key);
            auto segments = parser.parse();
            RETURN_IF(!segments.ok(), segments.status());

            auto segments_ptr = std::make_unique<std::vector<VariantPathSegmentPtr>>(std::move(segments.value()));
            auto* result = segments_ptr.get();
            path_cache[path_key] = std::move(segments_ptr);

            return result;
        }
    }
}

Status VariantFunctions::clear_variant_segments(FunctionContext* context, FunctionContext::FunctionStateScope scope){
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* variant_segments = reinterpret_cast<std::vector<VariantPathSegmentPtr>*>(context->get_function_state(scope));
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

    JsonPath stored_path;
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row) || json_path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto path = json_path_viewer.value(row);
        auto variant_segments_status = get_or_parse_variant_segments(context, path);
        if (!variant_segments_status.ok()) {
            VLOG(2) << "Failed to parse JSON path: " << path << ", error: " << variant_segments_status.status().to_string();
            result.append_null();
            continue;
        }

        auto variant_value = variant_viewer.value(row);
        Variant variant(variant_value->get_metadata(), variant_value->get_value());
        auto variant_result = VariantPathParser::seek(&variant, *variant_segments_status.value());
        if (!variant_result.ok()) {
            VLOG(2) << "Failed to query variant with path: " << path
                    << ", error: " << variant_result.status().to_string();
            result.append_null();
            continue;
        }

        cctz::time_zone zone = context->state()->timezone_obj();
        if (Status status = cast_variant_value_to<ResultType, false>(variant_result.value(), zone, result);!status.ok()) {
            result.append_null();
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks