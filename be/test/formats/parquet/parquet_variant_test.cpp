//
// Created by xavier bai on 2025/6/17.
//

#include <gtest/gtest.h>

#include <formats/parquet/variant.h>
#include <fs/fs.h>

#include "testutil/assert.h"

namespace starrocks::parquet {

class ParquetVariantTest : public testing::Test {

public:
    ParquetVariantTest() = default;
    ~ParquetVariantTest() override = default;

protected:
    uint8_t primitiveHeader(VariantPrimitiveType primitive) {
        return (static_cast<uint8_t>(primitive) << 2);
    }

    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        test_exec_dir = starrocks_home + "/be/test/formats/parquet/test_data/variant";

        _primitive_metadata_file_names = {
            "primitive_null.metadata",          "primitive_boolean_true.metadata",
            "primitive_boolean_false.metadata", "primitive_date.metadata",
            "primitive_decimal4.metadata",      "primitive_decimal8.metadata",
            "primitive_decimal16.metadata",     "primitive_float.metadata",
            "primitive_double.metadata",        "primitive_int8.metadata",
            "primitive_int16.metadata",         "primitive_int32.metadata",
            "primitive_int64.metadata",         "primitive_binary.metadata",
            "primitive_string.metadata",
        };
    }

protected:
    std::string test_exec_dir;
    std::vector<std::string> _primitive_metadata_file_names;
};

TEST_F(ParquetVariantTest, NullValue) {
    std::string_view empty_metadata(VariantMetadata::kEmptyMetadataChars, 3);
    const uint8_t null_chars[] = {primitiveHeader(VariantPrimitiveType::NULL_TYPE)};
    Variant variant{empty_metadata,
                         std::string_view{reinterpret_cast<const char*>(null_chars), 1}};
    EXPECT_EQ(VariantType::NULL_TYPE, variant.type());
}

TEST_F(ParquetVariantTest, VariantMetadata) {
    for (auto& test_file : _primitive_metadata_file_names) {
        std::cout << "Testing file: " << test_file << std::endl;

        std::string file_path = test_exec_dir + "/" + test_file;
        FileSystem* fs = FileSystem::Default();
        // Open the file using RandomAccessFile
        auto random_access_file = *fs->new_random_access_file(file_path);

        std::string content = *random_access_file->read_all();
        std::cout << "File content size: " << content.size() << std::endl;

        std::string_view metadata_buf{content};
        std::cout << "Metadata buffer size: " << metadata_buf.size() << std::endl;
        EXPECT_EQ(metadata_buf, VariantMetadata::kEmptyMetadata);

        VariantMetadata metadata(metadata_buf);
        // Only test get_key if metadata was successfully created
        EXPECT_EQ(metadata.get_key(0).status().message(), "Variant index out of range: 0 >= 0");
    }
}

}
