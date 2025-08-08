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

#include "column/column_builder.h"
#include "column/type_traits.h"
#include "common/statusor.h"
#include "formats/parquet/variant.h"
#include "types/logical_type.h"

namespace starrocks {

static StatusOr<RunTimeCppType<TYPE_BOOLEAN>> cast_variant_to_bool(const Variant& variant,
                                                                   ColumnBuilder<TYPE_BOOLEAN>& result);

template <LogicalType ResultType>
StatusOr<RunTimeColumnType<ResultType>> cast_variant_to_arithmetic(const Variant& variant,
                                                                   ColumnBuilder<ResultType>& result);

StatusOr<RunTimeCppType<TYPE_VARCHAR>> cast_variant_to_string(const Variant& variant, const VariantValue& value,
                                                              const cctz::time_zone& zone,
                                                              ColumnBuilder<TYPE_VARCHAR>& result);

template <LogicalType ResultType, bool AllowThrowException>
static Status cast_variant_value_to(const Variant& variant,
                                                                  const cctz::time_zone& zone,
                                                                  ColumnBuilder<ResultType>& result);

} // namespace starrocks