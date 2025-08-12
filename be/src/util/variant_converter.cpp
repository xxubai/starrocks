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

} // namespace starrocks