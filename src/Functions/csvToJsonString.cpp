#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractor.h>
#include <Functions/keyvaluepair/impl/KeyValuePairExtractorBuilder.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Poco/JSON/Object.h>
#include <Poco/NumberParser.h>
#include "Columns/ColumnString.h"
#include "FunctionHelpers.h"

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
}

namespace
{
/** Converts a CSV string to a JSON string.
  * The function takes two or three arguments:
  * - fieldNames: A comma-separated string of field names (must be a string constant)
  * - csvValue: A comma-separated string of values
  * - options (optional): A settings object that can customize the CSV parsing behavior (must be a string constant)
  *
  * The function returns a JSON string where the field names are mapped to their corresponding values.
  * The function attempts to detect the types of values and convert them appropriately:
  * - Numbers are converted to JSON numbers
  * - "true" and "false" are converted to JSON booleans
  * - All other values are treated as strings
  * 
  * Allowed options:
  * - delimiter: The delimiter character (default is ',')
  * - quote: The quote character (default is '"' or "'")
  * - null: The null representation (default is "\\N")
  *
  * Example:
  * csvToJSONString('name,age', 'John,42') = '{"name":"John","age":42}'
  * csvToJSONString('name|age', 'John|42', 'delimiter="|"') = '{"name":"John","age":42}'
  */
class FunctionCsvToJsonString final : public IFunction
{
    /** Splits a CSV string into a vector of strings according to the provided settings.
      * @param s The input CSV string to split
      * @param vectorBuffer A pre-allocated vector to store the split fields
      * @param settings CSV format settings that control parsing behavior (delimiter, quotes, etc)
      */
    static void splitCSV(const StringRef & s, std::vector<std::string> & vectorBuffer, const FormatSettings::CSV & settings)
    {
        ReadBufferFromMemory buf(s.data, s.size);
        vectorBuffer.clear();

        while (!buf.eof())
        {
            String field;
            readCSVString(field, buf, settings);
            vectorBuffer.push_back(field);

            // Skip delimiter if not at end
            if (!buf.eof() && *buf.position() == settings.delimiter)
                ++buf.position();
        }
    }

    /** Merges a vector of field names and values into a JSON String.
      * @param fieldNames A vector of field names
      * @param fieldValues A vector of field values
      * @param dest A reference to the destination stringstream to store the JSON result
      * @param detectTypes A boolean flag to determine if types should be detected and converted
      */
    static void mergeAsJSON(
        const std::vector<std::string> & fieldNames,
        const std::vector<std::string> & fieldValues,
        std::stringstream & dest,
        bool detectTypes)
    {
        const auto n = std::min(fieldNames.size(), fieldValues.size());
        Poco::JSON::Object json;

        for (size_t i = 0; i < n; ++i)
        {
            if (fieldNames.at(i).empty())
                continue;

            auto key = fieldNames.at(i);
            auto value = fieldValues.at(i);
            if (detectTypes)
            {
                double num;
                if (Poco::NumberParser::tryParseFloat(value, num))
                {
                    json.set(key, num);
                }
                else if (value == "true")
                {
                    json.set(key, true);
                }
                else if (value == "false")
                {
                    json.set(key, false);
                }
                else
                {
                    json.set(key, value);
                }
            }
            else
            {
                json.set(key, value);
            }
        }

        dest.str("");
        json.stringify(dest);
    }

    /** Parses CSV parsing options from a string and updates the settings.
      * @param optionsStr The input string containing CSV parsing options
      * @param settings Reference to the FormatSettings::CSV object to update
      */
    static void parseOptions(const std::string & optionsStr, FormatSettings::CSV & settings, bool & detectTypes)
    {
        auto config
            = KeyValuePairExtractorBuilder().withKeyValueDelimiter('=').withItemDelimiters({',', ';'}).withQuotingCharacter('"').build();

        auto keys = ColumnString::create();
        auto values = ColumnString::create();
        const auto numPairs = config->extract(optionsStr, keys, values);
        for (size_t i = 0; i < numPairs; ++i)
        {
            auto key = keys->getDataAt(i);
            auto value = values->getDataAt(i);

            if (key == "delimiter")
            {
                if (value.size == 1)
                {
                    settings.delimiter = value.data[0];
                }
                else
                {
                    settings.custom_delimiter = value.toString();
                }
            }
            else if (key == "quote")
            {
                // by default, we allow both single and double quotes...
                // ...specifying one will disable the other
                if (value.toString() == "'")
                {
                    settings.allow_double_quotes = false;
                }
                else if (value.toString() == "\"")
                {
                    settings.allow_single_quotes = false;
                }
                else
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Only single and double quotes are supported for quotechar in function {}", name);
                }
            }
            else if (key == "null")
            {
                settings.null_representation = value.toString();
            }
            else if (key == "autodetect_types")
            {
                if (value.toString() == "true" || value.toString() == "on") {
                    detectTypes = true;
                } else if (value.toString() == "false" || value.toString() == "off") {
                    detectTypes = false;
                } else {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid value for autodetect_types: {}", value.toString());
                }
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown option: {} in function {}", key.toString(), name);
            }
        }
    }

public:
    static constexpr auto name = "csvToJSONString";

    FunctionCsvToJsonString() = default;
    static FunctionPtr create(ContextPtr /* context */) { return std::make_shared<FunctionCsvToJsonString>(); }

    String getName() const override { return name; }

    // this function supports two or three arguments
    size_t getNumberOfArguments() const override { return 0; }
    virtual bool isVariadic() const override { return true; }

    // col:0 is the list of field names, col:2 is the options - both must be constant
    virtual ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeString>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!input_rows_count) return ColumnString::create(); // fast path for empty input
        
        std::stringstream dest;
        FormatSettings::CSV settings;
        bool detectTypes = true;

        // default settings
        settings.allow_single_quotes = true;
        settings.allow_double_quotes = true;
        settings.try_detect_header = false;
        settings.delimiter = ',';
        settings.null_representation = "\\N";

        // did we receive an option third argument with CSV parsing options?
        if (arguments.size() == 3)
        {
            if (const auto * constOptions = checkAndGetColumnConst<ColumnString>(arguments[2].column.get()); constOptions)
            {
                parseOptions(constOptions->getValue<String>(), settings, detectTypes);
            }
        }
        else if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Function {} supports 2 or 3 arguments", getName());
        }

        const auto & colFieldNames = arguments[0].column;

        // we expect the field names to be a string constant
        if (const auto * constFieldNames = checkAndGetColumnConst<ColumnString>(colFieldNames.get()); constFieldNames)
        {
            const auto fieldNamesStr = constFieldNames->getDataAt(0); 
            std::vector<std::string> fieldNames;
            fieldNames.reserve(128);

            // parse out the field names (just once)
            splitCSV(fieldNamesStr, fieldNames, settings);

            // csv value also provided as a string constant?
            if (const auto * constCSV = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()); constCSV)
            {
                std::vector<std::string> vectorBuffer;
                vectorBuffer.reserve(128);

                splitCSV(constCSV->getValue<String>(), vectorBuffer, settings);
                mergeAsJSON(fieldNames, vectorBuffer, dest, detectTypes);
                return DataTypeString().createColumnConst(input_rows_count, dest.str());
            }

            // csv value provided as a string column
            if (const auto * colCSV = checkAndGetColumn<ColumnString>(arguments[1].column.get()); colCSV)
            {
                std::vector<std::string> vectorBuffer;
                vectorBuffer.reserve(128);
                auto resultCol = ColumnString::create();

                for (size_t i = 0; i < input_rows_count; ++i)
                {
                    StringRef input = colCSV->getDataAt(i);
                    splitCSV(input, vectorBuffer, settings);
                    mergeAsJSON(fieldNames, vectorBuffer, dest, detectTypes);
                    resultCol->insertData(dest.str().data(), dest.str().length());
                }

                return resultCol;
            }
        }

        // we should never get here as the framework ensures that col:0 and col:2 are constant
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Function {} failed to validate arguments", getName());
    }
};

}

REGISTER_FUNCTION(csvToJSONString)
{
    factory.registerFunction<FunctionCsvToJsonString>();
}

}
