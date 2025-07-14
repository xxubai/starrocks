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
#include <cstdint>
#include <string_view>

#include "exprs/jsonpath.h"
#include "formats/parquet/variant.h"

namespace starrocks {

class VariantValue {
public:
    VariantValue(const std::string_view metadata, const std::string_view value)
            : _metadata(metadata), _value(value) {}

    explicit VariantValue(const Slice& slice) {
        const char* variant_raw = slice.get_data();
        // convert variant_raw to a string_view
        // The first 4 bytes are the size of the value
        uint32_t variant_size = *reinterpret_cast<const uint32_t*>(variant_raw);
        if (variant_size > slice.get_size() - sizeof(uint32_t)) {
            throw std::runtime_error("Invalid variant size");
        }

        const auto variant = std::string_view(variant_raw + sizeof(uint32_t), variant_size);
        _metadata = *load_metadata(variant);
        _value = std::string_view(variant_raw + sizeof(uint32_t) + _metadata.size(),
                                  variant_size - _metadata.size());
    }

    // Load metadata from the variant binary.
    // will slice the variant binary to extract metadata
    StatusOr<std::string_view> load_metadata(std::string_view variant) const;

    // Serialize the VariantValue to a byte array.
    // return the number of bytes written
    size_t serialize(uint8_t* dst) const;

    // Calculate the size of the serialized VariantValue.
    // 4 bytes for value size + metadata size + value size
    uint64_t serialize_size() const;

    // Convert to a JSON string
    StatusOr<std::string> to_json(cctz::time_zone timezone = cctz::local_time_zone()) const;
    StatusOr<std::string> to_string() const;

    std::string_view get_metadata() const { return _metadata; }
    std::string_view get_value() const { return _value; }

private:
    static constexpr uint8_t kVersionMask = 0b1111;
    static constexpr uint8_t kSortedStrings = 0b10000;
    static constexpr uint8_t kOffsetSizeMask = 0b11000000;
    static constexpr uint8_t kOffsetSizeShift = 6;
    static constexpr uint8_t kHeaderSize = 1;

    std::string_view _metadata;
    std::string_view _value;
};

}
