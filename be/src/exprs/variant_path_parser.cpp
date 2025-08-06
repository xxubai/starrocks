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

#include "variant_path_parser.h"

#include <cctype>
#include <stdexcept>

namespace starrocks {

bool VariantPathParser::is_at_end() const {
    return _pos >= _input.get_size();
}

char VariantPathParser::peek() const {
    if (is_at_end()) return '\0';
    return _input[_pos];
}

char VariantPathParser::advance() {
    if (is_at_end()) return '\0';
    return _input[_pos++];
}

bool VariantPathParser::match(char expected) {
    if (is_at_end() || peek() != expected) {
        return false;
    }

    advance();
    return true;
}

bool VariantPathParser::is_digit(char c) {
    return c >= '0' && c <= '9';
}

bool VariantPathParser::is_valid_key_char(char c) {
    // Valid unquoted key characters: letters, digits, underscore
    // Exclude dots and brackets which are delimiters
    return std::isalnum(c) || c == '_';
}

bool VariantPathParser::parse_root() {
    return match('$');
}

std::string VariantPathParser::parse_number() {
    std::string number;
    while (!is_at_end() && is_digit(peek())) {
        number += advance();
    }

    return number;
}

std::string VariantPathParser::parse_unquoted_key() {
    std::string key;
    while (!is_at_end() && is_valid_key_char(peek())) {
        key += advance();
    }

    return key;
}

std::string VariantPathParser::parse_quoted_string(char quote) {
    std::string str;
    while (!is_at_end() && peek() != quote) {
        char c = advance();
        // Handle escape sequences if needed
        if (c == '\\' && !is_at_end()) {
            char escaped = advance();
            switch (escaped) {
            case '"':
            case '\'':
            case '\\':
                str += escaped;
                break;
            case 'n':
                str += '\n';
                break;
            case 't':
                str += '\t';
                break;
            case 'r':
                str += '\r';
                break;
            default:
                str += escaped;
                break;
            }
        } else {
            str += c;
        }
    }

    return str;
}

StatusOr<VariantPathSegmentPtr> VariantPathParser::parse_array_index() {
    if (!match('[')) {
        return Status::VariantError(std::format("Expected '[' at position {}", _pos));
    }

    std::string indexStr = parse_number();
    if (indexStr.empty()) {
        return Status::VariantError(std::format("Expected array index after '[' at position {}", _pos));
    }

    if (!match(']')) {
        return Status::VariantError(std::format("Expected ']' after array index '{}' at position {}", indexStr, _pos));
    }

    try {
        int index = std::stoi(indexStr);
        return std::make_unique<ArrayExtraction>(index);
    } catch (const std::exception&) {
        return Status::VariantError(std::format("Invalid array index '{}' at position {}", indexStr, _pos));
    }
}

StatusOr<VariantPathSegmentPtr> VariantPathParser::parse_quoted_key() {
    if (!match('[')) {
        return Status::VariantError(std::format("Expected '[' at position {}", _pos));
    }

    char quote = peek();
    if (quote != '\'' && quote != '"') {
        return Status::VariantError(std::format("Expected quote (\" or ') at position {}, found '{}'", _pos, quote));
    }

    advance(); // consume quote

    std::string key = parse_quoted_string(quote);

    if (!match(quote)) {
        return Status::VariantError(
                std::format("Expected closing quote '{}' at position {}, found '{}'", quote, _pos, peek()));
    }

    if (!match(']')) {
        return Status::VariantError(std::format("Expected ']' after quoted key '{}' at position {}", key, _pos));
    }

    return std::make_unique<ObjectExtraction>(key);
}

StatusOr<VariantPathSegmentPtr> VariantPathParser::parse_object_key() {
    if (!match('.')) {
        return Status::VariantError(std::format("Expected '.' at position {}", _pos));
    }

    std::string key = parse_unquoted_key();
    if (key.empty()) {
        return Status::VariantError(std::format("Expected key after '.' at position {}", _pos));
    }

    return std::make_unique<ObjectExtraction>(key);
}

StatusOr<VariantPathSegmentPtr> VariantPathParser::parse_segment() {
    if (is_at_end()) {
        return Status::VariantError(std::format("Unexpected end of input at position {}", _pos));
    }

    if (char c = peek(); c == '.') {
        // Dot notation: .field
        return parse_object_key();
    } else if (c == '[') {
        // Bracket notation: could be [index] or ['key'] or ["key"]
        size_t saved_pos = _pos;

        // Try parsing as array index first
        auto arraySegment = parse_array_index();
        if (arraySegment.ok()) {
            return arraySegment;
        }

        // Reset and try parsing as quoted key
        _pos = saved_pos;
        auto objectSegment = parse_quoted_key();
        if (objectSegment.ok()) {
            return objectSegment;
        }

        // Reset position if both failed
        _pos = saved_pos;
        return Status::VariantError(std::format("Failed to parse segment at position {}", _pos));
    }

    return Status::VariantError(std::format("Unexpected character '{}' at position {}", peek(), _pos));
}

StatusOr<std::vector<VariantPathSegmentPtr>> VariantPathParser::parse() {
    _pos = 0;
    std::vector<VariantPathSegmentPtr> segments;

    // Must start with '$'
    if (!parse_root()) {
        return Status::InvalidArgument("Path must start with '$'");
    }

    // Parse segments until end of input
    while (!is_at_end()) {
        auto segment_result = parse_segment();
        if (!segment_result.ok()) {
            return segment_result.status();
        }

        segments.push_back(std::move(segment_result.value()));
    }

    return segments;
}

StatusOr<Variant> VariantPathParser::seek(const Variant* variant, const std::vector<VariantPathSegmentPtr>& segments) {
    Variant current = *variant;

    for (const auto& segment : segments) {
        if (segment->is_object_extraction()) {
            const auto* object_segment = static_cast<const ObjectExtraction*>(segment.get());
            auto result = current.get_object_by_key(object_segment->get_key());
            if (!result.ok()) {
                return Status::VariantError(
                        std::format("Object key '{}' not found in variant", object_segment->get_key()));
            }

            current = std::move(result.value());
        } else if (segment->is_array_extraction()) {
            const auto* array_segment = static_cast<const ArrayExtraction*>(segment.get());
            auto result = current.get_element_at_index(array_segment->get_index());
            if (!result.ok()) {
                return Status::VariantError(
                        std::format("Array index {} out of bounds in variant", array_segment->get_index()));
            }

            current = std::move(result.value());
        } else {
            return Status::VariantError("Unknown segment type");
        }
    }

    return current;
}

} // namespace starrocks
