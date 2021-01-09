package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("Hello, World!")

	// t := []string{
	// 	"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
	// 	"weather,location=us-midwest temperature=82",                     // no timestamp
	// 	"weather2,location=us-midwest,source=test-source temperature=82i,foo=12.3,bar=-1202.23 1465839830100400201"}

	// t := []string{
	// 	"weat\\,he\\ r,loc\\\"ation\\,\\ =us\\ mid\\\"west temperature=82,temperature_string=\"hot, really \\\"hot\\\"!\" 1465839830100400200", // all kinds of crazy characters
	// 	// "\"weather\",\"location\"=\"us-midwest\" \"temperature\"=82 1465839830100400200",                                                       // needlessly quoting of measurement, tag-keys, tag-values and field keys
	// }

	t := []string{
		// "weat\\=her,location=us-midwest temperature_string=\"temp: hot\" 1465839830100400200",           // escaped "=" in measurement
		"weat\\=her,loc\\=ation=us-mi\\=dwest temp\\=erature_string=\"temp\\=hot\" 1465839830100400201", // escaped "=" everywhere
	}

	// res := parse(strings.Join(t, "\n"))
	parse(strings.Join(t, "\n"))

	// fmt.Printf(fmt.Sprintf("%#v", res))
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

func parsePoint(line string) Point {
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
	timestamp := ""

	if r1.MatchString(line) {
		tokens := r1.FindStringSubmatch(line)

		if len(tokens) > 1 {
			measurementAndTagsStr = tokens[1]
		}

		if len(tokens) > 2 {
			fieldSetStr = tokens[2]
		}

		if len(tokens) > 3 {
			timestamp = tokens[3]
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
			timestamp = tokens[3]
		}
	} else {
		// invalid number of tokens
		// return nil
	}

	measurementAndTags := strings.Split(measurementAndTagsStr, ",")

	measurement := ArrayShift(&measurementAndTags)

	r := regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDSPACE))
	measurement = r.ReplaceAllString(measurement, " ")

	r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDCOMMA))
	measurement = r.ReplaceAllString(measurement, ",")

	r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDEQUAL))
	measurement = r.ReplaceAllString(measurement, "=")

	tagsStr := measurementAndTags

	tagSet := make(map[string]string)

	for _, tagStr := range tagsStr {

		r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDSPACE))
		tagStr = r.ReplaceAllString(tagStr, " ")

		r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDCOMMA))
		tagStr = r.ReplaceAllString(tagStr, ",")

		r = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDDBLQUOTE))
		tagStr = r.ReplaceAllString(tagStr, "\"")

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
			value = rf.ReplaceAllString(value.(string), "\"")

			rf = regexp.MustCompile(fmt.Sprintf("%v", ESCAPEDBACKSLASH))
			value = rf.ReplaceAllString(value.(string), "\\")

			key = rf.ReplaceAllString(key, "\\")

			// TODO: handle booleans

			// Try to convert the string to a float
			rf = regexp.MustCompile(`(\d+)[ui]`)

			if _, err := strconv.ParseFloat(value.(string), 64); err == nil {
				floatVal, _ := strconv.ParseFloat(value.(string), 64)
				value = floatVal
				fieldSet[key] = value.(float64)
			} else if rf.MatchString(value.(string)) {
				m := rf.FindAllString(value.(string), 1)
				v, e := strconv.ParseInt(m[1], 0, 64)
				fmt.Println(e)
				value = v
				fieldSet[key] = value.(int)
			} else {
				fieldSet[key] = value.(string)
			}
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

func index(slice []string, item string) int {
	for i, _ := range slice {
		if slice[i] == item {
			return i
		}
	}
	return -1
}
