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

#include "variant_converter.h"

#include "variant_util.h"

namespace starrocks {

StatusOr<RunTimeCppType<TYPE_BOOLEAN>> cast_variant_to_bool(const Variant& variant,
                                                            ColumnBuilder<TYPE_BOOLEAN>& result) {
    VariantType type = variant.type();
    if (type == VariantType::NULL_TYPE) {
        result.append_null();
        return Status::OK();
    }

    if (type == VariantType::BOOLEAN) {
        auto value = variant.get_bool();
        if (!value.ok()) {
            return value;
        }

        result.append(value.value());
        return Status::OK();
    }

    if (type == VariantType::STRING) {
        auto str = variant.get_string();
        if (str.ok()) {
            const char* str_value = str.value().data();
            size_t len = str.value().size();
            StringParser::ParseResult parsed;
            auto r = StringParser::string_to_int<int32_t>(str_value, len, &parsed);
            if (parsed != StringParser::PARSE_SUCCESS || std::isnan(r) || std::isinf(r)) {
                const bool casted = StringParser::string_to_bool(str_value, len, &parsed);
                if (parsed != StringParser::PARSE_SUCCESS) {
                    return Status::VariantError(fmt::format("Failed to cast string '{}' to BOOLEAN", str.value()));
                }

                result.append(casted);
            } else {
                result.append(r != 0);
            }

            return Status::OK();
        }
    }

    return Status::NotSupported(
            fmt::format("Cannot cast variant of type {} to boolean", VariantUtil::type_to_string(type)));
}

template <LogicalType ResultType>
StatusOr<RunTimeColumnType<ResultType>> cast_variant_to_arithmetic(const Variant& variant,
                                                                   ColumnBuilder<ResultType>& result) {
    VariantType type = variant.type();

    switch (type) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
    case VariantType::BOOLEAN: {
        auto value = variant.get_bool();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT8: {
        auto value = variant.get_int8();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT16: {
        auto value = variant.get_int16();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT32: {
        auto value = variant.get_int32();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    case VariantType::INT64: {
        auto value = variant.get_int64();
        if (!value.ok()) {
            return value.status();
        }

        result.append(static_cast<RunTimeCppType<ResultType>>(value.value()));
        return Status::OK();
    }
    default:
        return Status::NotSupported(fmt::format("Cannot cast variant of type {} to {}",
                                                VariantUtil::type_to_string(type), logical_type_to_string(ResultType)));
    }
}

StatusOr<RunTimeCppType<TYPE_VARCHAR>> cast_variant_to_string(const Variant& variant, const VariantValue& value,
                                                              const cctz::time_zone& zone,
                                                              ColumnBuilder<TYPE_VARCHAR>& result) {
    VariantType type = variant.type();
    switch (type) {
    case VariantType::NULL_TYPE: {
        result.append_null();
        return Status::OK();
    }
    case VariantType::STRING: {
        auto str = variant.get_string();
        if (!str.ok()) {
            return str.status();
        }

        result.append(str.value());
        return Status::OK();
    }
    default: {
        std::stringstream ss;
        Status status = VariantUtil::variant_to_json(value.get_metadata(), value.get_value(), ss, zone);
        if (!status.ok()) {
            return status;
        }

        result.append(ss.str());
        return Status::OK();
    }
    }
}

template <LogicalType ResultType, bool AllowThrowException>
StatusOr<RunTimeCppType<ResultType>> cast_variant_value_to(const VariantValue& value, const cctz::time_zone& zone,
                                                           ColumnBuilder<ResultType>& result) {
    if constexpr (!lt_is_arithmetic<ResultType> && !lt_is_string<ResultType> && ResultType != TYPE_VARIANT) {
        if constexpr (AllowThrowException) {
            return Status::NotSupported(
                    fmt::format("Cannot cast variant to type {}", logical_type_to_string(ResultType)));
        }

        result.append_null();
        return Status::OK();
    }

    if constexpr (ResultType == TYPE_VARIANT) {
        // Directly return the variant value
        result.append(std::move(value));
        return Status::OK();
    }

    Variant variant(value.get_metadata(), value.get_value());

    Status status;
    if constexpr (ResultType == TYPE_BOOLEAN) {
        status = cast_variant_to_bool(variant, result);
    } else if constexpr (lt_is_arithmetic<ResultType>) {
        status = cast_variant_to_arithmetic<ResultType>(variant, result);
    } else if constexpr (lt_is_string<ResultType>) {
        status = cast_variant_to_string(variant, value, zone, result);
    }

    if (!status.ok()) {
        if constexpr (AllowThrowException) {
            return Status::VariantError(fmt::format("Cannot cast variant to type {}: {}",
                                                    logical_type_to_string(ResultType), status.to_string()));
        } else {
            result.append_null();
        }
    }

    return Status::OK();
}

} // namespace starrocks