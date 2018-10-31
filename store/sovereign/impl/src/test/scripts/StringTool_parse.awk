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
    print "GroupID\tTestCase Name\tInput Strings\tChar Count\tStrings Processed\tString Types" \
            "\tChars Processed\tBytes Processed\tCPU Time\tUser Time\tElapsed Time\tGC Count\tGC Time" \
            "\tCompile Time\tCPS"
}

/\[[0-9]*\] Processing/ {
    groupTag=$1 " " $3
    next
}

/[^:]*: String generation/ {
    testCase=substr($1, 1, length($1) - 1)
    sourceStringCount=$5
    gsub(/,/, "", sourceStringCount)
    sourceCharCount=$8
    sub(/total=/, "", sourceCharCount)
    gsub(/,/, "", sourceCharCount)
    getline
}

$0 ~ testCase ": stringCount" {
    stringCount=$2
    sub(/stringCount=/, "", stringCount)
    gsub(/,/, "", stringCount)

    j=3
    typeCounts=""
    do {
        typeCounts = ( typeCounts == "" ? "" : typeCounts " " ) $(j++)
    } while ( substr(typeCounts, length(typeCounts) - 1) != "}," )
    sub(/typeCounts=/, "", typeCounts)
    gsub(/,/, "", typeCounts)

    charCount=$(j++)
    sub(/charCount=/, "", charCount)
    gsub(/,/, "", charCount)

    byteCount=$(j++)
    sub(/bytes=/, "", byteCount)
    gsub(/,/, "", byteCount)

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

    print groupTag "\t" testCase "\t" sourceStringCount "\t" sourceCharCount "\t" stringCount "\t" typeCounts "\t" charCount "\t" byteCount \
            "\t" cpuTime "\t" userTime "\t" elapsedTime "\t" gcCount "\t" gcTime "\t" compileTime "\t" cps
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
