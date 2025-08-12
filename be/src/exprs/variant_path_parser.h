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

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/statusor.h"
#include "formats/parquet/variant.h"
#include "util/slice.h"

namespace starrocks {

// Object key extraction like .field or ['field'] or ["field"]
class ObjectExtraction {
private:
    std::string _key;

public:
    explicit ObjectExtraction(std::string key) : _key(std::move(key)) {}

    const std::string& get_key() const { return _key; }
};

// Array index extraction like [123]
class ArrayExtraction {
private:
    int _index;

public:
    explicit ArrayExtraction(int index) : _index(index) {}

    int get_index() const { return _index; }
};

// Use variant instead of polymorphic pointers
using VariantPathExtraction = std::variant<ObjectExtraction, ArrayExtraction>;

struct VariantPath {
    std::vector<VariantPathExtraction> segments;

    explicit VariantPath(std::vector<VariantPathExtraction> segments) : segments(std::move(segments)) {}

    VariantPath() = default;
    VariantPath(VariantPath&&) = default;
    VariantPath(const VariantPath& rhs) = default;
    ~VariantPath() = default;

    void reset(const VariantPath& rhs);
    void reset(VariantPath&& rhs);

    // Seek into a variant using the parsed segments
    static StatusOr<Variant> seek(const Variant* variant, const VariantPath* variant_path);
};

struct NativeVariantPath {
    VariantPath variant_path;
};

// Parser for variant path expressions
class VariantPathParser {
public:
    // Parse a JSON path string and return segments vector
    static StatusOr<VariantPath> parse(Slice input);
    static StatusOr<VariantPath> parse(const std::string& input);

private:
    // Internal parser state for static methods
    struct ParserState {
        Slice input;
        size_t pos = 0;

        explicit ParserState(Slice inp) : input(inp) {}

        bool is_at_end() const;
        char peek() const;
        char advance();
        bool match(char expected);
    };

    static bool is_digit(char c);
    static bool is_valid_key_char(char c);

    // Parser methods
    static bool parse_root(ParserState& state);
    static StatusOr<VariantPathExtraction> parse_segment(ParserState& state);
    static StatusOr<ArrayExtraction> parse_array_index(ParserState& state);
    static StatusOr<ObjectExtraction> parse_object_key(ParserState& state);
    static StatusOr<ObjectExtraction> parse_quoted_key(ParserState& state);
    static std::string parse_number(ParserState& state);
    static std::string parse_unquoted_key(ParserState& state);
    static std::string parse_quoted_string(ParserState& state, char quote);
};

} // namespace starrocks
