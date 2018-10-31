#!/usr/bin/awk -f
##
## Converts the output of StringTool_full to a flat text file suitable for importing into a spreadsheet.
##
## @param <filename> console output from StringTool_full
##
## @see StringTool_full
## @see StringTool_main
##

BEGIN {
    print "TestCase Name\tInput Records" \
            "\tInput Cells\tMost Cells/Record\tLeast Cells/Record" \
            "\tInput Versionss\tMost Versions/Record\tLeast Versions/Record" \
            "\tRecords Processed\tCells Processed\tBytes Processed" \
            "\tCPU Time\tUser Time\tElapsed Time\tGC Count\tGC Time\tCompile Time" \
            "\tcells/sec"
}

/[^:]*: Record generation/ {
    testCase=substr($1, 1, length($1) - 1)
    testPattern = testCase
    sub(/\[/, "\\[", testPattern)
    sub(/\]/, "\\]", testPattern)
    sourceRecordCount=$5
    gsub(/,/, "", sourceRecordCount)
    sourceCellCount = cleanNumber("total=", $8)
    sourceMostCellCount = cleanNumber("most=", $9)
    sourceLeastCellCount = cleanNumber("least=", $10)
    sourceVersionCount = cleanNumber("total=", $12)
    sourceMostVersionCount = cleanNumber("most=", $13)
    sourceLeastVersionCount = cleanNumber("least=", $14)
    getline
}

$0 ~ testPattern ": versions" {
    versionCount = cleanNumber("versions=", $2)
    cellCount = cleanNumber("cells=", $3)
    byteCount = cleanNumber("bytes=", $4)

    j=5

    cpuTime = parseTime("cpuTime")
    userTime = parseTime("userTime")
    elapsedTime = parseTime("elapsedTime")

    gcDetail=""
    do {
        gcDetail = ( gcDetail == "" ? "" : gcDetail " " ) $(j++)
    } while ( substr(gcDetail, length(gcDetail) - 1) != "}," )
    sub(/^GC{/, "", gcDetail)
    sub(/},$/, "", gcDetail)
    gsub(/,/, "", gcDetail)
    gcTime = convertTime(substr(gcDetail, match(gcDetail, /time=/) + 5))
    gcCount = substr(gcDetail, 7, match(gcDetail, / /) - 7)

    compileTime = parseTime("compileTime")

    cps=$(j++)
    gsub(/,/, "", cps)

    print testCase "\t" sourceRecordCount \
            "\t" sourceCellCount "\t" sourceMostCellCount "\t" sourceLeastCellCount \
            "\t" sourceVersionCount "\t" sourceMostVersionCount "\t" sourceLeastVersionCount \
            "\t" versionCount "\t" cellCount "\t" byteCount \
            "\t" cpuTime "\t" userTime "\t" elapsedTime "\t" gcCount "\t" gcTime "\t" compileTime "\t" cps
}


##
## Cleans a 'tagged' number value
##
function cleanNumber(propertyName, value,    temp) {
    temp = value
    sub(propertyName, "", temp)
    gsub(/[^0-9]/, "", temp)
    ## gsub(/,/, "", temp)
    return temp
}

##
## Parse a time value in the form of "<propertyName>=[[<hours> h ]<minutes> m ]<seconds>.<fraction> s"
## returning seconds.
##
function parseTime(propertyName,    value, tokens, i, m, time) {
    value = ""
    do {
        value = ( value == "" ? "" : value " " ) $(j++)
    } while ( substr(value, length(value)) != "," )
    sub(propertyName "=", "", value)
    sub(/,/, "", value)

    return convertTime(value)
}

##
## Converts a time value in the form of "[[<hours> h ]<minutes> m ]<seconds><fraction> s" to seconds.
##
function convertTime(value,    tokens, i, m, time) {
    time = 0
    m = 0
    for (i = split(value, tokens); i > 0; i--) {
        if (m != 0) {
            time = time + (m * tokens[i])
            m = 0
        } else if (tokens[i] == "s") {
            m = 1
        } else if (tokens[i] == "m") {
            m = 60
        } else if (tokens[i] == "h") {
            m = 60 * 60
        }
    }
    return time
}
