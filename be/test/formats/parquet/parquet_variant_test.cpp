//
// Created by xavier bai on 2025/6/17.
//

#include <gtest/gtest.h>

#include <formats/parquet/variant.h>

namespace starrocks::parquet {

class ParquetVariantTest : public testing::Test {

public:
    ParquetVariantTest() = default;
    ~ParquetVariantTest() override = default;

protected:
    uint8_t primitiveHeader(VariantPrimitiveType primitive) {
        return (static_cast<uint8_t>(primitive) << 2);
    }
};

TEST_F(ParquetVariantTest, NullValue) {
    std::string_view empty_metadata(VariantMetadata::kEmptyMetadataChars, 3);
    const uint8_t null_chars[] = {primitiveHeader(VariantPrimitiveType::NULL_TYPE)};
    Variant variant{empty_metadata,
                         std::string_view{reinterpret_cast<const char*>(null_chars), 1}};
    EXPECT_EQ(VariantType::NULL_TYPE, variant.type());
}

}
