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

#pragma once

#include "column/column.h"
#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "common/statusor.h"
#include "formats/parquet/variant.h"
#include "function_helper.h"
#include "types/logical_type.h"
#include "util/variant_converter.h"

namespace starrocks {

class VariantFunctions {
public:
    /**
     * @param: [variant, json_path, result_type]
     * @paramType: [VariantColumn, BinaryColumn, BinaryColumn]
     * @return: result column with type `result_type`
     */
    DEFINE_VECTORIZED_FN(variant_query);

private:
    template <LogicalType ResultType>
    static StatusOr<ColumnPtr> _do_variant_query(FunctionContext* context, const Columns& vector);
};

} // namespace starrocks