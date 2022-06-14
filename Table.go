package importer

import (
	"fmt"
	"strings"

	"cloud.google.com/go/civil"
)

type Granularity string

const (
	GranularityNone  Granularity = ""
	GranularityDay   Granularity = "day"
	GranularityWeek  Granularity = "week"
	GranularityMonth Granularity = "month"
)

type Table struct {
	Name        string
	Granularity Granularity
	Schema      interface{}
	Append      *TableAppend
	Replace     *TableReplace
	Merge       *TableMerge
	Truncate    *TableTruncate
}

type where struct {
	FieldName       string
	Operator        string
	ValueExpression string
	isRaw           bool
}

type TableAppend struct {
}

type TableReplace struct {
	wheres []where
}

type TableMerge struct {
	JoinFields        []string
	DoNotUpdateFields []string
}

type TableTruncate struct {
}

func TableReplaceDummy() *TableReplace {
	var tableReplace *TableReplace = &TableReplace{}
	tableReplace.AddDummy()
	return tableReplace
}

func (tableReplace *TableReplace) Clear() *TableReplace {
	tableReplace.wheres = []where{}
	return tableReplace
}

func (tableReplace *TableReplace) AddDummy() *TableReplace {
	tableReplace.wheres = append(tableReplace.wheres, where{"1", "=", "1", false})
	return tableReplace
}

func (tableReplace *TableReplace) AddWhereRaw(expression string) *TableReplace {
	tableReplace.wheres = append(tableReplace.wheres, where{"", "", expression, true})
	return tableReplace
}

func (tableReplace *TableReplace) AddWhere(fieldName string, operator string, valueExpression string) *TableReplace {
	tableReplace.wheres = append(tableReplace.wheres, where{fieldName, operator, valueExpression, false})
	return tableReplace
}

func (tableReplace *TableReplace) AddWhereDate(fieldName string, date civil.Date) *TableReplace {
	tableReplace.wheres = append(tableReplace.wheres, where{fieldName, "=", fmt.Sprintf("'%s'", date.String()), false})
	return tableReplace
}

func (tableReplace *TableReplace) AddWhereDateRange(fieldName string, startDate civil.Date, endDate civil.Date) *TableReplace {
	tableReplace.wheres = append(tableReplace.wheres, where{fieldName, "BETWEEN", fmt.Sprintf("'%s' AND '%s'", startDate.String(), endDate.String()), false})
	return tableReplace
}

func (tableReplace *TableReplace) AddWhereDates(fieldName string, dates []civil.Date) {
	if len(dates) == 0 {
		return
	}

	var datesString []string

	for _, date := range dates {
		datesString = append(datesString, date.String())
	}

	tableReplace.wheres = append(tableReplace.wheres, where{fieldName, "IN", fmt.Sprintf("('%s')", strings.Join(datesString, "','")), false})
}

func (tableReplace *TableReplace) WhereString() *string {
	if len(tableReplace.wheres) == 0 {
		return nil
	}

	whereStrings := []string{}

	for _, where := range tableReplace.wheres {
		if where.isRaw {
			whereStrings = append(whereStrings, where.ValueExpression)
			continue
		}

		fieldName := strings.Trim(where.FieldName, " ")
		if fieldName == "" {
			continue
		}
		operator := strings.Trim(where.Operator, " ")
		if operator == "" {
			operator = "="
		}
		valueExpression := strings.Trim(where.ValueExpression, " ")
		if valueExpression == "" {
			continue
		}

		whereStrings = append(whereStrings, fmt.Sprintf("%s %s %s", fieldName, operator, valueExpression))
	}

	if len(whereStrings) == 0 {
		return nil
	}

	whereString := strings.Join(whereStrings, " AND ")
	return &whereString
}
