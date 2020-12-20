package main

import (
	"fmt"
	"regexp"
)

func main() {
	testStr := "weather2,location=us-midwest,source=test-source temperature=82i,foo=12.3,bar=-1202.23 1465839830100400201"
	re := regexp.MustCompile(`/^(.*?) (.*) (.*)$/`)
	fmt.Println(re.FindAllString(testStr, 1))

}
