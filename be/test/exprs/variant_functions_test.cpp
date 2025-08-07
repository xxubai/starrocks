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

#include <glog/logging.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "column/column.h"
#include "column/const_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/mock_vectorized_expr.h"
#include "gtest/gtest-param-test.h"
#include "gutil/casts.h"
#include "gutil/strings/strip.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/defer_op.h"
#include "formats/parquet/variant.h"
#include "util/variant_converter.h"
#include "fs/fs.h"

namespace starrocks {

class VariantFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        // Setup test data directory
        std::string starrocks_home = getenv("STARROCKS_HOME");
        test_exec_dir = starrocks_home + "/be/test/exec";
        variant_test_data_dir = starrocks_home + "/be/test/formats/parquet/test_data/variant";
    }

protected:
    // Helper function to read variant test data from parquet test files
    std::pair<std::string, std::string> load_variant_test_data(const std::string& metadata_file,
                                                               const std::string& value_file) {
        FileSystem* fs = FileSystem::Default();

        auto metadata_path = variant_test_data_dir + "/" + metadata_file;
        auto value_path = variant_test_data_dir + "/" + value_file;

        auto metadata_file_obj = *fs->new_random_access_file(metadata_path);
        auto value_file_obj = *fs->new_random_access_file(value_path);

        std::string metadata_content = *metadata_file_obj->read_all();
        std::string value_content = *value_file_obj->read_all();

        return {std::move(metadata_content), std::move(value_content)};
    }

    // Helper function to create VariantValue from test data files
    VariantValue create_variant_from_test_data(const std::string& metadata_file, const std::string& value_file) {
        auto [metadata, value] = load_variant_test_data(metadata_file, value_file);
        return VariantValue(std::string_view(metadata), std::string_view(value));
    }

    // Helper function to create simple variant values for basic tests
    VariantValue create_simple_variant(const std::string& json_str) {
        if (json_str == "null" || json_str == "NULL") {
            return VariantValue::of_null();
        }

        // For simple integer values using test data
        if (json_str == "42") {
            return create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value");
        }

        if (json_str == "1") {
            // Create a simple int8 variant manually for value 1
            std::string_view empty_metadata = VariantMetadata::kEmptyMetadata;
            uint8_t int_chars[] = {static_cast<uint8_t>(VariantPrimitiveType::INT8 << 2), 1};
            std::string_view value_data(reinterpret_cast<const char*>(int_chars), sizeof(int_chars));
            return VariantValue(empty_metadata, value_data);
        }

        // For boolean values using test data
        if (json_str == "true") {
            return create_variant_from_test_data("primitive_boolean_true.metadata", "primitive_boolean_true.value");
        }

        if (json_str == "false") {
            return create_variant_from_test_data("primitive_boolean_false.metadata", "primitive_boolean_false.value");
        }

        // For string values using test data
        if (json_str == R"("hello")") {
            return create_variant_from_test_data("short_string.metadata", "short_string.value");
        }

        // Default to null for unsupported cases in this test
        return VariantValue::of_null();
    }

public:
    TExprNode expr_node;
    std::string test_exec_dir;
    std::string variant_test_data_dir;
};

// Test cases using real variant test data
class VariantQueryTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string, std::string>> {};

TEST_P(VariantQueryTestFixture, variant_query_with_test_data) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    std::string metadata_file = std::get<0>(GetParam());
    std::string value_file = std::get<1>(GetParam());
    std::string param_path = std::get<2>(GetParam());
    std::string param_result = std::get<3>(GetParam());

    // Create variant value using test data files
    VariantFunctionsTest test_helper;
    VariantValue variant_value = test_helper.create_variant_from_test_data(metadata_file, value_file);
    variant_column->append(variant_value);

    if (param_path == "NULL") {
        path_builder.append_null();
    } else {
        path_builder.append(param_path);
    }

    Columns columns{variant_column, path_builder.build(true)};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    StripWhiteSpace(&param_result);
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto variant_result = datum.get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        StripWhiteSpace(&variant_str);
        ASSERT_EQ(param_result, variant_str);
    }
}

// Test cases using real variant test data from parquet test files
INSTANTIATE_TEST_SUITE_P(
        VariantQueryTestWithRealData, VariantQueryTestFixture,
        ::testing::Values(
                // clang-format off
                // Basic primitive tests using real test data
                std::make_tuple("primitive_boolean_true.metadata", "primitive_boolean_true.value", "$", "true"),
                std::make_tuple("primitive_boolean_false.metadata", "primitive_boolean_false.value", "$", "false"),
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "$", "42"),
                std::make_tuple("primitive_int16.metadata", "primitive_int16.value", "$", "1234"),
                std::make_tuple("primitive_int32.metadata", "primitive_int32.value", "$", "123456"),
                std::make_tuple("primitive_int64.metadata", "primitive_int64.value", "$", "1234567890123456789"),
                std::make_tuple("primitive_float.metadata", "primitive_float.value", "$", "1234567940.0"),
                std::make_tuple("primitive_double.metadata", "primitive_double.value", "$", "1234567890.1234"),
                std::make_tuple("short_string.metadata", "short_string.value", "$", "\"Less than 64 bytes (❤️ with utf8)\""),
                std::make_tuple("primitive_string.metadata", "primitive_string.value", "$", "\"This string is longer than 64 bytes and therefore does not fit in a short_string and it also includes several non ascii characters such as 🐢, 💖, ♥️, 🎣 and 🤦!!\""),

                // Object and array tests
                std::make_tuple("object_primitive.metadata", "object_primitive.value", "$.int_field", "1"),
                std::make_tuple("object_nested.metadata", "object_nested.value", "$.nested_object.nested_field", "\"nested_value\""),
                std::make_tuple("array_primitive.metadata", "array_primitive.value", "$.array_field[0]", "1"),
                std::make_tuple("array_nested.metadata", "array_nested.value", "$.nested_array[0].nested_field", "\"nested_value\""),

                // Non-existent path tests
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "$.nonexistent", "NULL"),
                std::make_tuple("primitive_string.metadata", "primitive_string.value", "$.missing", "NULL"),

                // Null path tests
                std::make_tuple("primitive_int8.metadata", "primitive_int8.value", "NULL", "NULL")
                // clang-format on
                ));

// Simplified test cases for basic functionality using simple variant values
class VariantQuerySimpleTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string>> {};

TEST_P(VariantQuerySimpleTestFixture, variant_query_simple) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    ColumnBuilder<TYPE_VARCHAR> path_builder(1);

    std::string param_variant = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    std::string param_result = std::get<2>(GetParam());

    // Create variant value using simplified approach
    VariantFunctionsTest test_helper;
    VariantValue variant_value = test_helper.create_simple_variant(param_variant);
    variant_column->append(variant_value);

    if (param_path == "NULL") {
        path_builder.append_null();
    } else {
        path_builder.append(param_path);
    }

    Columns columns{variant_column, path_builder.build(true)};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    StripWhiteSpace(&param_result);
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto variant_result = datum.get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        StripWhiteSpace(&variant_str);
        ASSERT_EQ(param_result, variant_str);
    }
}

TEST_F(VariantFunctionsTest, variant_query_invalid_arguments) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Test with no arguments
    {
        Columns columns;
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }

    // Test with one argument
    {
        auto variant_column = VariantColumn::create();
        Columns columns{variant_column};
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }

    // Test with three arguments
    {
        auto variant_column = VariantColumn::create();
        auto path_column = BinaryColumn::create();
        auto extra_column = BinaryColumn::create();
        Columns columns{variant_column, path_column, extra_column};
        auto result = VariantFunctions::variant_query(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_TRUE(result.status().is_invalid_argument());
    }
}

TEST_F(VariantFunctionsTest, variant_query_null_columns) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Test with all null columns
    auto variant_column = NullableColumn::create(VariantColumn::create(), NullColumn::create());
    auto path_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());

    variant_column->append_nulls(2);
    path_column->append_nulls(2);

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(2, result->size());
    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
}

TEST_F(VariantFunctionsTest, variant_query_invalid_path) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    auto path_column = BinaryColumn::create();

    // Create variant value using test data
    VariantValue variant_value = create_variant_from_test_data("primitive_int8.metadata", "primitive_int8.value");
    variant_column->append(variant_value);

    // Invalid path syntax
    path_column->append("$.invalid..path");

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());
    ASSERT_TRUE(result->is_null(0));
}

TEST_F(VariantFunctionsTest, variant_query_complex_types) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    auto path_column = BinaryColumn::create();

    // Test with object data
    VariantValue variant_value = create_variant_from_test_data("object_primitive.metadata", "object_primitive.value");
    variant_column->append(variant_value);
    path_column->append("$.int_field");

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());
    ASSERT_FALSE(result->is_null(0));

    auto variant_result = result->get(0).get_variant();
    ASSERT_TRUE(!!variant_result);
    auto json_result = variant_result->to_json();
    ASSERT_TRUE(json_result.ok());
    std::string variant_str = json_result.value();
    ASSERT_EQ("1", variant_str);
}

TEST_F(VariantFunctionsTest, variant_query_multiple_rows) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    auto path_column = BinaryColumn::create();

    // Create multiple variant values using test data
    std::vector<std::pair<std::string, std::string>> test_files = {
        {"primitive_int8.metadata", "primitive_int8.value"},
        {"primitive_boolean_true.metadata", "primitive_boolean_true.value"},
        {"short_string.metadata", "short_string.value"}
    };

    for (const auto& [metadata_file, value_file] : test_files) {
        VariantValue variant_value = create_variant_from_test_data(metadata_file, value_file);
        variant_column->append(variant_value);
        path_column->append("$");
    }

    Columns columns{variant_column, path_column};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(3, result->size());

    std::vector<std::string> expected_results = {"42", "true", "\"Less than 64 bytes (❤️ with utf8)\""};
    for (size_t i = 0; i < 3; ++i) {
        ASSERT_FALSE(result->is_null(i));
        auto variant_result = result->get(i).get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        ASSERT_EQ(expected_results[i], variant_str);
    }
}

TEST_F(VariantFunctionsTest, variant_query_const_columns) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto variant_column = VariantColumn::create();
    auto path_column = BinaryColumn::create();

    // Create variant value using test data
    VariantValue variant_value = create_variant_from_test_data("short_string.metadata", "short_string.value");
    variant_column->append(variant_value);
    path_column->append("$");

    // Create const columns
    auto const_variant = ConstColumn::create(variant_column, 3);
    auto const_path = ConstColumn::create(path_column, 3);

    Columns columns{const_variant, const_path};

    ColumnPtr result = VariantFunctions::variant_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(3, result->size());

    for (size_t i = 0; i < 3; ++i) {
        ASSERT_FALSE(result->is_null(i));
        auto variant_result = result->get(i).get_variant();
        ASSERT_TRUE(!!variant_result);
        auto json_result = variant_result->to_json();
        ASSERT_TRUE(json_result.ok());
        std::string variant_str = json_result.value();
        ASSERT_EQ("\"Less than 64 bytes (❤️ with utf8)\"", variant_str);
    }
}

} // namespace starrocks
