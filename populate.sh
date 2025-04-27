#!/bin/bash

SOURCE=https://databank.worldbank.org/data/download/WDI_CSV.zip
DATABASE=database.duckdb

echo "Downloading WDI data from World Bank..."

TMPDIR=$(mktemp -d)
echo "Temporary directory created at $TMPDIR"

FILENAME=$(rclone copyurl $SOURCE $TMPDIR --auto-filename --print-filename)
echo "Downloaded file: $FILENAME"
echo "Unzipping file..."
unzip -d $TMPDIR $TMPDIR/$FILENAME
echo "Unzipped files:"
ls -lh $TMPDIR

echo "Copying files to DuckDB..."

if [ -f $DATABASE ]; then
		echo "Database $DATABASE already exists. Deleting it..."
		rm $DATABASE
fi

for i in $TMPDIR/*.csv; do
echo "Processing file: $i"
duckdb $DATABASE <<EOF
-- Create a table for the CSV file
CREATE TABLE IF NOT EXISTS $(basename $i .csv | tr -d '-') AS
	SELECT * FROM read_csv_auto('$i', header = TRUE);
EOF
done

echo "Data copied to DuckDB."
echo "Cleaning up temporary files..."
rm -rf $TMPDIR
echo "Temporary files cleaned up."
echo "Done."


