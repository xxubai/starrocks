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

#include "exprs/variant_path_parser.h"

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

namespace starrocks {

class VariantPathParserTest : public ::testing::TestWithParam<std::string> {
public:
    static std::vector<std::string> get_test_paths() {
        return {
            "$",                    // Root only
            "$.name",              // Simple field access
            "$.field1.field2",     // Nested field access
            "$[0]",                // Array index
            "$[123]",              // Array index with larger number
            "$.field[0]",          // Mixed access
            "$['quoted_key']",     // Single quoted key
            "$[\"double_quoted\"]", // Double quoted key
            "$.arr[0].field['key']" // Complex path
        };
    }
};

TEST_P(VariantPathParserTest, ParseSuccess) {
    std::string path = GetParam();
    VariantPathParser parser(path);

    auto result = parser.parse();

    ASSERT_TRUE(result.ok()) << "Failed to parse path: " << path;

    if (path != "$") {
        EXPECT_GT(result->size(), 0) << "Expected segments for path: " << path;
    }
}

TEST_P(VariantPathParserTest, ParseSegmentTypes) {
    std::string path = GetParam();
    VariantPathParser parser(path);

    auto result = parser.parse();
    ASSERT_TRUE(result.ok()) << "Failed to parse path: " << path;

    for (const auto& segment : *result) {
        ASSERT_TRUE(segment != nullptr) << "Segment should not be null for path: " << path;

        bool isObject = segment->is_object_extraction();
        bool isArray = segment->is_array_extraction();
        EXPECT_TRUE(isObject || isArray) << "Segment should be either object or array extraction for path: " << path;
        EXPECT_FALSE(isObject && isArray) << "Segment cannot be both object and array extraction for path: " << path;
    }
}

INSTANTIATE_TEST_SUITE_P(
    PathParsingTests,
    VariantPathParserTest,
    ::testing::ValuesIn(VariantPathParserTest::get_test_paths()),
    [](const ::testing::TestParamInfo<std::string>& info) {
        std::string name = info.param;

        std::replace(name.begin(), name.end(), '$', '_');
        std::replace(name.begin(), name.end(), '.', '_');
        std::replace(name.begin(), name.end(), '[', '_');
        std::replace(name.begin(), name.end(), ']', '_');
        std::replace(name.begin(), name.end(), '\'', '_');
        std::replace(name.begin(), name.end(), '"', '_');
        std::replace(name.begin(), name.end(), '/', '_');

        if (!name.empty() && name[0] == '_') {
            name = name.substr(1);
        }

        return name.empty() ? "root" : name;
    }
);

class VariantPathParserBasicTest : public ::testing::Test {};

TEST_F(VariantPathParserBasicTest, InvalidPathsReturnNullopt) {
    const std::vector<std::string> invalidPaths = {
        "",                     // Empty string
        "invalid",             // No $ prefix
        "$.",                  // Incomplete dot notation
        "$[",                  // Incomplete bracket
        "$[]",                 // Empty brackets
        "$[abc]",              // Invalid array index
        "$['unclosed",         // Unclosed quote
        "$.field[",            // Incomplete bracket after field
    };

    for (const auto& path : invalidPaths) {
        VariantPathParser parser(path);
        auto result = parser.parse();
        EXPECT_FALSE(result.ok()) << "Should fail to parse invalid path: " << path;
    }
}

} // namespace starrocks