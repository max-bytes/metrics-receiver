package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("Hello, World!")

	t := []string{
		"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
		"weather,location=us-midwest temperature=82",                     // no timestamp
		"weather2,location=us-midwest,source=test-source temperature=82i,foo=12.3,bar=-1202.23 1465839830100400201"}

	res := parse(strings.Join(t, "\n"))

	fmt.Printf(fmt.Sprintf("%#v", res))
}

func parse(input string) []Point {

	lines := strings.Split(input, "\n")
	var ret []Point

	for _, line := range lines {
		if line == " " {
			continue
		}

		if string(line[0]) == "#" {
			continue
		}

		point := parsePoint(line)

		// if point != (Point{}) {

		// }
		ret = append(ret, point)
	}

	return ret
}

// func parsePoint(line string) Point {
func parsePoint(line string) Point {
	const ESCAPEDSPACE = "___ESCAPEDSPACE___"
	const ESCAPEDCOMMA = "___ESCAPEDCOMMA___"
	const ESCAPEDEQUAL = "___ESCAPEDEQUAL___" // MODIFICATION
	const ESCAPEDDBLQUOTE = "___ESCAPEDDBLQUOTE___"
	const ESCAPEDBACKSLASH = "___ESCAPEDBACKSLASH___"

	re := regexp.MustCompile("/\\\\ /")
	line = re.ReplaceAllString(line, ESCAPEDSPACE)

	re = regexp.MustCompile("/\\\\,/")
	line = re.ReplaceAllString(line, ESCAPEDCOMMA)

	re = regexp.MustCompile("/\\\\=/")
	line = re.ReplaceAllString(line, ESCAPEDEQUAL) // MODIFICATION

	re = regexp.MustCompile(`/\\\\"/`) // added a \ beffore " here  !!!
	line = re.ReplaceAllString(line, ESCAPEDDBLQUOTE)

	re = regexp.MustCompile("/\\\\\\\\/")
	line = re.ReplaceAllString(line, ESCAPEDBACKSLASH)

	r1 := regexp.MustCompile("/^(.*?) (.*) (.*)$/")
	r2 := regexp.MustCompile("/^(.*?) (.*)$/")
	// r1_match, _ :=
	// r2_match, _ := regexp.MatchString("/^(.*?) (.*)$/", line)

	measurementAndTagsStr := ""
	fieldSetStr := ""
	timestamp := ""
	var t = r1.MatchString(line)
	var t1 = r2.MatchString(line)
	var t2 = r2.FindString(line)

	fmt.Println(t)
	fmt.Println(t1)
	fmt.Println(t2)
	if r1.MatchString(line) {
		tokens := r1.FindAllString(line, 1)
		measurementAndTagsStr = tokens[1]
		fieldSetStr = tokens[2]
		timestamp = tokens[3]
	} else if r2.MatchString(line) {
		tokens := r2.FindAllString(line, 1)
		measurementAndTagsStr = tokens[1]
		fieldSetStr = tokens[2]
		timestamp = tokens[3]
	} else {
		// invalid number of tokens
		// return nil
	}

	fmt.Println(timestamp)
	measurementAndTags := strings.Split(measurementAndTagsStr, ",")

	measurement := ArrayShift(&measurementAndTags)
	// measurement

	// 	$measurementAndTags = explode(',', $measurementAndTagsStr);
	// 	$measurement = array_shift($measurementAndTags);

	r := regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDSPACE))
	measurement = r.ReplaceAllString(measurement, " ")

	r = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDCOMMA))
	measurement = r.ReplaceAllString(measurement, ",")

	r = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDEQUAL))
	measurement = r.ReplaceAllString(measurement, "=")

	tagsStr := measurementAndTags

	tagSet := make(map[string]string)

	for _, tagStr := range tagsStr {

		r = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDSPACE))
		tagStr = r.ReplaceAllString(tagStr, " ")

		r = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDCOMMA))
		r = regexp.MustCompile("/$ESCAPEDCOMMA/")
		tagStr = r.ReplaceAllString(tagStr, ",")

		r = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDDBLQUOTE))
		tagStr = r.ReplaceAllString(tagStr, "\"")

		tagKV := strings.Split(tagStr, "=")

		if len(tagKV) == 2 {
			tagKey := tagKV[0]
			tagValue := tagKV[1]

			r = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDEQUAL))
			tagStr = r.ReplaceAllString(tagKey, "=")

			tagStr = r.ReplaceAllString(tagValue, "=")

			// k, _ := strconv.ParseInt(tagKey, 0, 64)
			tagSet[tagKey] = tagValue
		}

	}

	// cut out quoted strings and replace them with placeholders (will be inserted back in later)

	var strs []string

	if strings.Index(fieldSetStr, `"`) != 0 { // check this again!!
		cnt := 0
		rs := regexp.MustCompile(`/"(.*?)"/`)
		fieldSetStr = rs.ReplaceAllStringFunc(fieldSetStr, func(matches string) string {
			strs[0] = matches
			cnt = cnt + 1
			return `___ESCAPEDSTRING_` + strconv.Itoa(cnt) + `___`
		})
	}

	fieldSetArray := strings.Split(fieldSetStr, ",")
	fieldSet := make(map[string]interface{})

	for _, fieldStr := range fieldSetArray {

		rf := regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDSPACE))
		fieldStr = rf.ReplaceAllString(fieldStr, " ")

		rf = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDCOMMA))
		fieldStr = rf.ReplaceAllString(fieldStr, ",")

		fieldKV := strings.Split(fieldStr, "=")

		if len(fieldKV) == 2 {
			key := fieldKV[0]
			var value interface{}
			value = fieldKV[1]

			// insert previously cut out quoted strings again

			rf = regexp.MustCompile(`/___ESCAPEDSTRING_(\d+)___/`)
			fieldSetStr = rf.ReplaceAllStringFunc(fieldSetStr, func(matches string) string {
				// return strs[matches]
				return ""
			})

			rf = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDEQUAL))
			key = rf.ReplaceAllString(key, "=")
			value = r.ReplaceAllString(value.(string), "=")

			rf = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDDBLQUOTE))
			value = r.ReplaceAllString(value.(string), "\"")

			rf = regexp.MustCompile(fmt.Sprintf("/%v/", ESCAPEDBACKSLASH))
			value = r.ReplaceAllString(value.(string), "\\")
			key = r.ReplaceAllString(key, "\\")

			// TODO: handle booleans

			// Try to convert the string to a float
			rf = regexp.MustCompile(`/(\d+)[ui]/`)

			if _, err := strconv.Atoi(value.(string)); err == nil {
				floatVal, _ := strconv.ParseFloat(value.(string), 64)
				value = floatVal
			} else if rf.MatchString(value.(string)) {
				m := rf.FindAllString(value.(string), 1)
				v, _ := strconv.ParseInt(m[1], 0, 64)
				value = v
			}

			// k, _ := strconv.ParseInt(key, 0, 64)
			fieldSet[key] = value.(string) // check this again!!
		}
	}

	return Point{measurement, fieldSet, tagSet, timestamp}
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
