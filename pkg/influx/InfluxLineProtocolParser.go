package influx

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"mhx.at/gitlab/landscape/metrics-receiver-ng/pkg/general"
)

func Parse(input string, currentTimestamp time.Time) ([]general.Point, error) {

	input = strings.ReplaceAll(input, "\r", "")

	lines := strings.Split(input, "\n")
	var ret []general.Point = make([]general.Point, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		if line[0] == '#' {
			continue
		}

		point, error := ParsePoint(line, currentTimestamp)

		if error != nil {
			return nil, error
		}

		ret = append(ret, point)
	}

	return ret, nil
}

const ESCAPEDSPACE = "___ESCAPEDSPACE___"
const ESCAPEDCOMMA = "___ESCAPEDCOMMA___"
const ESCAPEDEQUAL = "___ESCAPEDEQUAL___" // MODIFICATION
const ESCAPEDDBLQUOTE = "___ESCAPEDDBLQUOTE___"
const ESCAPEDBACKSLASH = "___ESCAPEDBACKSLASH___"

var regexEscapedSpaceForward = regexp.MustCompile(`\\ `)
var regexEscapedCommaForward = regexp.MustCompile(`\\,`)
var regexEscapedEqualForward = regexp.MustCompile(`\\=`)
var regexEscapedDoubleQuoteForward = regexp.MustCompile(`\\\"`)
var regexEscapedBackslashForward = regexp.MustCompile(`\\\\`)
var regexLineVariant1 = regexp.MustCompile("^(.*?) (.*) (.*)$")
var regexLineVariant2 = regexp.MustCompile("^(.*?) (.*)$")
var regexEscapedSpaceBackward = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDSPACE))
var regexEscapedCommaBackward = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDCOMMA))
var regexEscapedEqualBackward = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDEQUAL))
var regexEscapedDoubleQuoteBackward = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDDBLQUOTE))
var regexEscapedBackslashBackward = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDBACKSLASH))
var regexInt = regexp.MustCompile(`(\d+)[ui]`)
var regexEscapedQuotedStringForward = regexp.MustCompile(`"(.*?)"`)
var regexEscapedQuotedStringBackward = regexp.MustCompile(`___ESCAPEDSTRING_(\d+)___`)

func ParsePoint(line string, currentTime time.Time) (general.Point, error) {

	line = regexEscapedSpaceForward.ReplaceAllString(line, ESCAPEDSPACE)
	line = regexEscapedCommaForward.ReplaceAllString(line, ESCAPEDCOMMA)
	line = regexEscapedEqualForward.ReplaceAllString(line, ESCAPEDEQUAL) // MODIFICATION
	line = regexEscapedDoubleQuoteForward.ReplaceAllString(line, ESCAPEDDBLQUOTE)
	line = regexEscapedBackslashForward.ReplaceAllString(line, ESCAPEDBACKSLASH)

	measurementAndTagsStr := ""
	fieldSetStr := ""
	timestampStr := ""

	if regexLineVariant1.MatchString(line) {
		tokens := regexLineVariant1.FindStringSubmatch(line)

		if len(tokens) > 1 {
			measurementAndTagsStr = tokens[1]
		}

		if len(tokens) > 2 {
			fieldSetStr = tokens[2]
		}

		if len(tokens) > 3 {
			timestampStr = tokens[3]
		}
	} else if regexLineVariant2.MatchString(line) {
		tokens := regexLineVariant2.FindStringSubmatch(line)

		if len(tokens) > 1 {
			measurementAndTagsStr = tokens[1]
		}

		if len(tokens) > 2 {
			fieldSetStr = tokens[2]
		}

		if len(tokens) > 3 {
			timestampStr = tokens[3]
		}
	} else {
		// invalid number of tokens
		return general.Point{}, errors.New("invalid number of tokens")
	}

	measurementAndTags := strings.Split(measurementAndTagsStr, ",")

	measurement := ArrayShift(&measurementAndTags)

	measurement = regexEscapedSpaceBackward.ReplaceAllString(measurement, " ")

	measurement = regexEscapedCommaBackward.ReplaceAllString(measurement, ",")

	measurement = regexEscapedEqualBackward.ReplaceAllString(measurement, "=")

	measurement = regexEscapedDoubleQuoteBackward.ReplaceAllString(measurement, "\"")

	tagsStr := measurementAndTags

	tagSet := make(map[string]string)

	for _, tagStr := range tagsStr {

		tagStr = regexEscapedSpaceBackward.ReplaceAllString(tagStr, " ")
		tagStr = regexEscapedCommaBackward.ReplaceAllString(tagStr, ",")
		tagStr = regexEscapedDoubleQuoteBackward.ReplaceAllString(tagStr, "\"")
		tagStr = regexEscapedBackslashBackward.ReplaceAllString(tagStr, "\\")

		tagKV := strings.Split(tagStr, "=")

		if len(tagKV) == 2 {
			tagKey := tagKV[0]
			tagValue := tagKV[1]

			tagKey = regexEscapedEqualBackward.ReplaceAllString(tagKey, "=")
			tagValue = regexEscapedEqualBackward.ReplaceAllString(tagValue, "=")

			tagSet[tagKey] = tagValue
		}

	}

	// cut out quoted strings and replace them with placeholders (will be inserted back in later)

	var strs []string
	if strings.Index(fieldSetStr, `"`) != 0 {
		cnt := 0

		fieldSetStr = regexEscapedQuotedStringForward.ReplaceAllStringFunc(fieldSetStr, func(matches string) string {
			t := regexEscapedQuotedStringForward.FindStringSubmatch(fieldSetStr)
			strs = append(strs, t[1])
			result := `___ESCAPEDSTRING_` + strconv.Itoa(cnt) + `___`
			cnt = cnt + 1
			return result
		})
	}

	fieldSetArray := strings.Split(fieldSetStr, ",")
	fieldSet := make(map[string]interface{})

	for _, fieldStr := range fieldSetArray {

		fieldStr = regexEscapedSpaceBackward.ReplaceAllString(fieldStr, " ")

		fieldStr = regexEscapedCommaBackward.ReplaceAllString(fieldStr, ",")

		fieldKV := strings.Split(fieldStr, "=")

		if len(fieldKV) == 2 {
			key := fieldKV[0]
			value := fieldKV[1]

			// insert previously cut out quoted strings again

			value = regexEscapedQuotedStringBackward.ReplaceAllStringFunc(value, func(matches string) string {
				t := regexEscapedQuotedStringBackward.FindStringSubmatch(value)
				index, _ := strconv.Atoi(t[1])
				return strs[index]
			})

			key = regexEscapedEqualBackward.ReplaceAllString(key, "=")
			value = regexEscapedEqualBackward.ReplaceAllString(value, "=")

			key = regexEscapedDoubleQuoteBackward.ReplaceAllString(key, "\"")
			value = regexEscapedDoubleQuoteBackward.ReplaceAllString(value, "\"")

			value = regexEscapedBackslashBackward.ReplaceAllString(value, "\\")
			key = regexEscapedBackslashBackward.ReplaceAllString(key, "\\")

			// TODO: handle booleans

			// Try to convert the string to a float
			if result, err := strconv.Atoi(value); err == nil {
				fieldSet[key] = result
			} else if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
				// floatVal, _ := strconv.ParseFloat(value.(string), 64)
				fieldSet[key] = floatVal
			} else if regexInt.MatchString(value) {
				m := regexInt.FindStringSubmatch(value)
				v, e := strconv.ParseInt(m[1], 10, 64)
				if e != nil {
					return general.Point{}, e
				}
				fieldSet[key] = v
			} else {
				fieldSet[key] = value
			}
		}
	}

	// build a proper timestamp: parse if set, set to current time if not set
	var timestamp time.Time
	if timestampStr != "" {
		t, _ := strconv.Atoi(timestampStr)
		timestamp = time.Unix(0, int64(t))
	} else {
		timestamp = currentTime
	}

	return general.Point{Measurement: measurement, Fields: fieldSet, Tags: tagSet, Timestamp: timestamp}, nil
}

func ArrayShift(s *[]string) string {
	if len(*s) == 0 {
		// return nil
		return ""
	}
	f := (*s)[0]
	*s = (*s)[1:]
	return f
}
