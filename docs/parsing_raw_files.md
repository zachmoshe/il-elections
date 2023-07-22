# Parsing raw files

Raw data files come in various formats and encodings and require some careful handling (e.g. columns renaming, strings cleaning, etc...). The `il_elections.data.parsers` module contains implementations for all the parsers we currently need.

All parsers return data as a `pd.DataFrame` with a defined set of columns ([il_elections.data.data.BallotsMetadata](/il_elections/data/data.py) or [il_elections.data.data.BallotsVotes](/il_elections/data/data.py)).


## Campaigns Configuration

The mapping of all campaigns to their data location and relevant parsers is [here](/config/preprocessing_config.yaml).

## Available Parsers

### `BallotsMetadataExcelParser`

Handles metadata files that come as an Excel file. Column names are in hebrew and are mapped to the
canonical english names. Handles conversion of fields to strings, and Excel's auto-formatting of
numbers as floats (locality_id).

### `BallotsMetadataPDFParser`

Handles metadata files where the table was provided as a multi-page PDF with a table per page.
This parser loads all tables as dataframes, concats them together and cleans some fields as reading
them from the PDF added spaces and reversed the hebrew string.

**Important Notice:** Due to technical problems with parsing RTL text directily with the PDF parsing
package, this parser, although named "PDFParser", actually relies on a manual conversion of the PDF
to Excel file, and then reads and parses the Excel file. Notice that the parsing and handling are
different than the regular Excel parser because of the structure of the tables and many other
modifications that this format uses. In the `data` folder you'll find both the `pdf` and the
`pdf.xlsx` files. PDF files appeared only once, and hopefully this was a one-time effort.

### `BallotsVotesFileParser`

Handles votes files (both CSV and Excel formats). In case of a CSV file, also receive the encoding
as it differs from one campaign to another. It appears that the dump is manual and the encoding
depends on the machine it was done from. Either `cp1255` (windows) or `utf8` (newer windows or
mac/linux?). The parser handles different variations of the same column name.
