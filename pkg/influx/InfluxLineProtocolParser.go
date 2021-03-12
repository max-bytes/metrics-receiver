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
	var ret []general.Point

	for _, line := range lines {
		if line == "" {
			continue
		}

		if string(line[0]) == "#" {
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

func ParsePoint(line string, currentTime time.Time) (general.Point, error) {
	const ESCAPEDSPACE = "___ESCAPEDSPACE___"
	const ESCAPEDCOMMA = "___ESCAPEDCOMMA___"
	const ESCAPEDEQUAL = "___ESCAPEDEQUAL___" // MODIFICATION
	const ESCAPEDDBLQUOTE = "___ESCAPEDDBLQUOTE___"
	const ESCAPEDBACKSLASH = "___ESCAPEDBACKSLASH___"

	re := regexp.MustCompile("\\\\ ")
	line = re.ReplaceAllString(line, ESCAPEDSPACE)

	re = regexp.MustCompile("\\\\,")
	line = re.ReplaceAllString(line, ESCAPEDCOMMA)

	re = regexp.MustCompile(`\\=`)
	line = re.ReplaceAllString(line, ESCAPEDEQUAL) // MODIFICATION

	re = regexp.MustCompile(`\\\"`)
	line = re.ReplaceAllString(line, ESCAPEDDBLQUOTE)

	re = regexp.MustCompile("\\\\\\\\")
	line = re.ReplaceAllString(line, ESCAPEDBACKSLASH)

	r1 := regexp.MustCompile("^(.*?) (.*) (.*)$")
	r2 := regexp.MustCompile("^(.*?) (.*)$")

	measurementAndTagsStr := ""
	fieldSetStr := ""
	timestampStr := ""

	if r1.MatchString(line) {
		tokens := r1.FindStringSubmatch(line)

		if len(tokens) > 1 {
			measurementAndTagsStr = tokens[1]
		}

		if len(tokens) > 2 {
			fieldSetStr = tokens[2]
		}

		if len(tokens) > 3 {
			timestampStr = tokens[3]
		}
	} else if r2.MatchString(line) {
		tokens := r2.FindStringSubmatch(line)

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

	r := regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDSPACE))
	measurement = r.ReplaceAllString(measurement, " ")

	r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDCOMMA))
	measurement = r.ReplaceAllString(measurement, ",")

	r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDEQUAL))
	measurement = r.ReplaceAllString(measurement, "=")

	r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDDBLQUOTE))
	measurement = r.ReplaceAllString(measurement, "\"")

	tagsStr := measurementAndTags

	tagSet := make(map[string]string)

	for _, tagStr := range tagsStr {

		r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDSPACE))
		tagStr = r.ReplaceAllString(tagStr, " ")

		r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDCOMMA))
		tagStr = r.ReplaceAllString(tagStr, ",")

		r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDDBLQUOTE))
		tagStr = r.ReplaceAllString(tagStr, "\"")

		r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDBACKSLASH))
		tagStr = r.ReplaceAllString(tagStr, "\\")

		tagKV := strings.Split(tagStr, "=")

		if len(tagKV) == 2 {
			tagKey := tagKV[0]
			tagValue := tagKV[1]

			r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDEQUAL))
			tagKey = r.ReplaceAllString(tagKey, "=")

			tagValue = r.ReplaceAllString(tagValue, "=")

			tagSet[tagKey] = tagValue
		}

	}

	// cut out quoted strings and replace them with placeholders (will be inserted back in later)

	var strs []string
	if strings.Index(fieldSetStr, `"`) != 0 {
		cnt := 0
		rs := regexp.MustCompile(`"(.*?)"`)
		fieldSetStr = rs.ReplaceAllStringFunc(fieldSetStr, func(matches string) string {
			t := rs.FindStringSubmatch(fieldSetStr)
			strs = append(strs, t[1])
			result := `___ESCAPEDSTRING_` + strconv.Itoa(cnt) + `___`
			cnt = cnt + 1
			return result
		})
	}

	fieldSetArray := strings.Split(fieldSetStr, ",")
	fieldSet := make(map[string]interface{})

	for _, fieldStr := range fieldSetArray {

		rf := regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDSPACE))
		fieldStr = rf.ReplaceAllString(fieldStr, " ")

		rf = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDCOMMA))
		fieldStr = rf.ReplaceAllString(fieldStr, ",")

		fieldKV := strings.Split(fieldStr, "=")

		if len(fieldKV) == 2 {
			key := fieldKV[0]
			var value interface{}
			value = fieldKV[1]

			// insert previously cut out quoted strings again

			rf = regexp.MustCompile(`___ESCAPEDSTRING_(\d+)___`)
			value = rf.ReplaceAllStringFunc(value.(string), func(matches string) string {
				t := rf.FindStringSubmatch(value.(string))
				index, _ := strconv.Atoi(t[1])
				return strs[index]
			})

			rf = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDEQUAL))
			key = rf.ReplaceAllString(key, "=")
			value = rf.ReplaceAllString(value.(string), "=")

			rf = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDDBLQUOTE))
			key = rf.ReplaceAllString(key, "\"")
			value = rf.ReplaceAllString(value.(string), "\"")

			rf = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDBACKSLASH))
			value = rf.ReplaceAllString(value.(string), "\\")

			key = rf.ReplaceAllString(key, "\\")

			// TODO: handle booleans

			// Try to convert the string to a float
			rf = regexp.MustCompile(`(\d+)[ui]`)

			if result, err := strconv.Atoi(value.(string)); err == nil {
				value = result
				fieldSet[key] = value.(int)
			} else if _, err := strconv.ParseFloat(value.(string), 64); err == nil {
				floatVal, _ := strconv.ParseFloat(value.(string), 64)
				value = floatVal
				fieldSet[key] = value.(float64)
			} else if rf.MatchString(value.(string)) {
				m := rf.FindStringSubmatch(value.(string))
				v, e := strconv.ParseInt(m[1], 10, 64)
				value = v
				if e != nil {
					return general.Point{}, e
				}
				fieldSet[key] = value.(int64)
			} else {
				fieldSet[key] = value.(string)
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

func index(slice []string, item string) int {
	for i := range slice {
		if slice[i] == item {
			return i
		}
	}
	return -1
}
