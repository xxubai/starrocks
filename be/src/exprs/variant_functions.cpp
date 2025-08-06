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

namespace starrocks {

StatusOr<ColumnPtr> VariantFunctions::variant_query(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (columns.size() != 2) {
        return Status::InvalidArgument("VariantFunctions::variant_query requires 2 arguments");
    }

    return _do_variant_query<TYPE_VARIANT>(context, columns);
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
        VariantPathParser parser(path);
        auto variant_segments = parser.parse();
        if (!variant_segments.ok()) {
            VLOG(2) << "Failed to parse JSON path: " << path << ", error: " << variant_segments.status().to_string();
            result.append_null();
            continue;
        }

        auto variant_value = variant_viewer.value(row);
        Variant variant(variant_value->get_metadata(), variant_value->get_value());
        auto variant_result = parser.seek(&variant, variant_segments.value());
        if (!variant_result.ok()) {
            VLOG(2) << "Failed to query variant with path: " << path
                    << ", error: " << variant_result.status().to_string();
            result.append_null();
            continue;
        }

        cctz::time_zone zone = context->state()->timezone_obj();
        Status status = cast_variant_value_to<ResultType, false>(variant_value, zone, result);
        if (!status.ok()) {
            result.append_null();
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks