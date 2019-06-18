package canonical

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

type Transform func(string) string

func Strings(transforms ...Transform) StringCanonicalizer {
	return StringCanonicalizer{
		t: transforms,
		m: map[string]mapped{},
	}
}

type StringCanonicalizer struct {
	m map[string]mapped
	t []Transform
}

type mapped struct {
	s string
	c int
}

func (c StringCanonicalizer) Canonicalize(s string) string {
	for _, t := range c.t {
		s = t(s)
	}

	key := strings.ToLower(s)

	m:=c.m[key]
	m.s = s
	m.c ++

	if m.c > 1 {
		s = fmt.Sprintf("%s_%d", m.s, m.c)
	}
	c.m[key]=m
	return s
}

var alphanumRegex = regexp.MustCompile(`[^A-z0-9]+`)

func ToAlphanumeric(str string) string {
	return alphanumRegex.ReplaceAllString(str, " ")
}

func ToPascalCase(str string) string {
	str = strings.TrimSpace(str)
	if utf8.RuneCountInString(str) < 2 {
		return str
	}

	var buff strings.Builder
	var prev string
	for _, r := range str {
		c := string(r)
		if c != " " {
			if  prev == ""|| prev == " "{
				c = strings.ToUpper(c)
			}
			buff.WriteString(c)
		}
		prev = c
	}
	return buff.String()
}