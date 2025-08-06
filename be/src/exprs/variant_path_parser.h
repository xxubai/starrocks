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

#include <memory>
#include <string>
#include <vector>

#include "common/statusor.h"
#include "formats/parquet/variant.h"
#include "util/slice.h"

namespace starrocks {

// Base class for variant path segments
class VariantPathSegment {
public:
    virtual ~VariantPathSegment() = default;
    virtual bool is_object_extraction() const = 0;
    virtual bool is_array_extraction() const = 0;
};

// Object key extraction like .field or ['field'] or ["field"]
class ObjectExtraction : public VariantPathSegment {
private:
    std::string _key;

public:
    explicit ObjectExtraction(const std::string& key) : _key(key) {}

    bool is_object_extraction() const override { return true; }
    bool is_array_extraction() const override { return false; }

    const std::string& get_key() const { return _key; }
};

// Array index extraction like [123]
class ArrayExtraction : public VariantPathSegment {
private:
    int _index;

public:
    explicit ArrayExtraction(int index) : _index(index) {}

    bool is_object_extraction() const override { return false; }
    bool is_array_extraction() const override { return true; }

    int get_index() const { return _index; }
};

using VariantPathSegmentPtr = std::unique_ptr<VariantPathSegment>;

// Parser for variant path expressions
class VariantPathParser {
public:
    explicit VariantPathParser(const Slice& input) : _input(input), _pos(0) {}

    explicit VariantPathParser(const std::string& input) : _input(input), _pos(0) {}

    VariantPathParser() : _input(Slice()), _pos(0) {}

    VariantPathParser(const VariantPathParser&) = default;
    VariantPathParser(VariantPathParser&&) = default;
    ~VariantPathParser() = default;

    // Parse a JSON path string and return array of segments
    StatusOr<std::vector<VariantPathSegmentPtr>> parse();

    // Seek into a variant using the parsed segments
    // Returns a StatusOr<Variant> which contains the result or an error status
    static StatusOr<Variant> seek(const Variant* variant, const std::vector<VariantPathSegmentPtr>& segments);

private:
    Slice _input;
    size_t _pos;

    bool is_at_end() const;
    char peek() const;
    char advance();
    bool match(char expected);
    static bool is_digit(char c);
    static bool is_valid_key_char(char c);

    // Parser methods
    bool parse_root();
    StatusOr<VariantPathSegmentPtr> parse_segment();
    StatusOr<VariantPathSegmentPtr> parse_array_index();
    StatusOr<VariantPathSegmentPtr> parse_object_key();
    StatusOr<VariantPathSegmentPtr> parse_quoted_key();
    std::string parse_number();
    std::string parse_unquoted_key();
    std::string parse_quoted_string(char quote);
};

} // namespace starrocks
