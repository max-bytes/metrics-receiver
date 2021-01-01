package Influx

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// func main() {
// 	fmt.Println("Hello, World!")

// 	t := []string{
// 		"weather,location=us-midwest temperature=82 1465839830100400200", // basic line
// 		"weather,location=us-midwest temperature=82",                     // no timestamp
// 		"weather2,location=us-midwest,source=test-source temperature=82i,foo=12.3,bar=-1202.23 1465839830100400201"}

// 	parse(strings.Join(t, "\n"))
// }

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
	line = re.ReplaceAllString(ESCAPEDSPACE, line)

	re = regexp.MustCompile("/\\\\,/")
	line = re.ReplaceAllString(ESCAPEDCOMMA, line)

	re = regexp.MustCompile("/\\\\=/")
	line = re.ReplaceAllString(ESCAPEDEQUAL, line) // MODIFICATION

	re = regexp.MustCompile(`/\\\\"/`) // added a \ beffore " here  !!!
	line = re.ReplaceAllString(ESCAPEDDBLQUOTE, line)

	re = regexp.MustCompile("/\\\\\\\\/")
	line = re.ReplaceAllString(ESCAPEDBACKSLASH, line)

	r1 := regexp.MustCompile("/^(.*?) (.*) (.*)$/")
	r2 := regexp.MustCompile("/^(.*?) (.*)$/")
	// r1_match, _ :=
	// r2_match, _ := regexp.MatchString("/^(.*?) (.*)$/", line)

	measurementAndTagsStr := ""
	fieldSetStr := ""
	timestamp := ""

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

	var tagSet []string

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

			k, _ := strconv.ParseInt(tagKey, 0, 64)
			tagSet[k] = tagValue
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
	var fieldSet []string

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

			k, _ := strconv.ParseInt(key, 0, 64)
			fieldSet[k] = value.(string) // check this again!!
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

// public function parse(input string) : array {

// $lines = explode("\n", $input);

// $ret = [];
// foreach($lines as $line) {
// 	if ($line === '') continue; // ignore empty lines
// 	if ($line[0] === '#') continue; // comments are lines that start with '#', ignore

// 	$point = $this->parsePoint($line);
// 	if ($point !== null)
// 		$ret[] = $point;
// }
// return $ret;
// }

// private function parsePoint(string $line): Point {

// 	// parsing loosely based on https://metacpan.org/source/DOMM/InfluxDB-LineProtocol-1.012/lib/InfluxDB/LineProtocol.pm#L168
// 	// explicit modifications from the perl implementation are marked with a comment "MODIFICATION"

// 	$ESCAPEDSPACE = "___ESCAPEDSPACE___";
// 	$ESCAPEDCOMMA = "___ESCAPEDCOMMA___";
// 	$ESCAPEDEQUAL = "___ESCAPEDEQUAL___"; // MODIFICATION
// 	$ESCAPEDDBLQUOTE = "___ESCAPEDDBLQUOTE___";
// 	$ESCAPEDBACKSLASH = "___ESCAPEDBACKSLASH___";
// 	$line = preg_replace("/\\\\ /", $ESCAPEDSPACE, $line);
// 	$line = preg_replace("/\\\\,/", $ESCAPEDCOMMA, $line);
// 	$line = preg_replace("/\\\\=/", $ESCAPEDEQUAL, $line); // MODIFICATION
// 	$line = preg_replace('/\\\\"/', $ESCAPEDDBLQUOTE, $line);
// 	$line = preg_replace("/\\\\\\\\/", $ESCAPEDBACKSLASH, $line);

// 	if (preg_match("/^(.*?) (.*) (.*)$/", $line, $tokens)) {
// 		$measurementAndTagsStr = $tokens[1];
// 		$fieldSetStr = $tokens[2];
// 		$timestamp = $tokens[3];
// 	} else if (preg_match("/^(.*?) (.*)$/", $line, $tokens)) {
// 		$measurementAndTagsStr = $tokens[1];
// 		$fieldSetStr = $tokens[2];
// 		$timestamp = null;//sprintf("%d",microtime(true) * 1000000000); // current time in nanoseconds, TODO: is this the proper way?
// 	} else {
// 		return null; // invalid number of tokens
// 	}

// 	$measurementAndTags = explode(',', $measurementAndTagsStr);
// 	$measurement = array_shift($measurementAndTags);
// 	$measurement = preg_replace("/$ESCAPEDSPACE/", ' ', $measurement);
// 	$measurement = preg_replace("/$ESCAPEDCOMMA/", ',', $measurement);
// 	$measurement = preg_replace("/$ESCAPEDEQUAL/", '=', $measurement); // MODIFICATION
// 	$tagsStr = $measurementAndTags;

// 	$tagSet = [];
// 	foreach($tagsStr as $tagStr) {
// 		$tagStr = preg_replace("/$ESCAPEDSPACE/", ' ', $tagStr);
// 		$tagStr = preg_replace("/$ESCAPEDCOMMA/", ',', $tagStr);

// 		$tagStr = preg_replace("/$ESCAPEDDBLQUOTE/", "\"", $tagStr); // MODIFICATION

// 		$tagKV = explode("=", $tagStr);
// 		if (sizeof($tagKV) == 2) {
// 			$tagKey = $tagKV[0];
// 			$tagValue = $tagKV[1];
// 			$tagKey = preg_replace("/$ESCAPEDEQUAL/", '=', $tagKey); // MODIFICATION
// 			$tagValue = preg_replace("/$ESCAPEDEQUAL/", '=', $tagValue); // MODIFICATION
// 			$tagSet[$tagKey] = $tagValue;
// 		}
// 	}

// 	// cut out quoted strings and replace them with placeholders (will be inserted back in later)
// 	$strings = [];
// 	if (strpos($fieldSetStr, '"') !== false) {
// 		$cnt = 0;
// 		$fieldSetStr = preg_replace_callback('/"(.*?)"/', function($matches) use (&$cnt, &$strings) {
// 			$strings[] = $matches[1];
// 			return '___ESCAPEDSTRING_'.$cnt++.'___';
// 		}, $fieldSetStr);
// 	}

// 	$fieldSetArray = explode(',', $fieldSetStr);
// 	$fieldSet = [];
// 	foreach($fieldSetArray as $fieldStr) {
// 		$fieldStr = preg_replace("/$ESCAPEDSPACE/", ' ', $fieldStr);
// 		$fieldStr = preg_replace("/$ESCAPEDCOMMA/", ',', $fieldStr);
// 		$fieldKV = explode("=", $fieldStr);
// 		if (sizeof($fieldKV) == 2) {
// 			$key = $fieldKV[0];
// 			$value = $fieldKV[1];

// 			// insert previously cut out quoted strings again
// 			$value = preg_replace_callback('/___ESCAPEDSTRING_(\d+)___/', function($matches) use (&$strings) {
// 				return $strings[$matches[1]];
// 			}, $value);

// 			$key = preg_replace("/$ESCAPEDEQUAL/", '=', $key); // MODIFICATION
// 			$value = preg_replace("/$ESCAPEDEQUAL/", '=', $value); // MODIFICATION

// 			$value = preg_replace("/$ESCAPEDDBLQUOTE/", "\"", $value);
// 			$value = preg_replace("/$ESCAPEDBACKSLASH/", "\\", $value);
// 			$key = preg_replace("/$ESCAPEDBACKSLASH/", '\\', $key);

// 			// TODO: handle booleans

// 			// Try to convert the string to a float
// 			if (is_numeric($value)) {
// 				$floatVal = floatval($value);
// 				$value = $floatVal;
// 			} else if (preg_match('/(\d+)[ui]/', $value, $matches)) {
// 				// handle signed and unsigned integer (have suffix i or u)
// 				$value = intval($matches[1]);
// 			}

// 			$fieldSet[$key] = $value;
// 		}
// 	}

// 	return new Point($measurement, $fieldSet, $tagSet, $timestamp);
// }
