// Copyright (c) 2016 Twitch Interactive

package mocks

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var (
	// If mockDynamoErrorTrigger is passed in as the table name in a MockDynamo
	// request, then the MockDynamo will respond with nil output and
	// mockDynamoError.
	mockDynamoErrorTrigger = "error-trigger"

	// items per page when scanning
	mockDynamoPageSize = 5
)

func errInternalError() error {
	return awserr.New("InternalFailure", "triggered error", nil)
}

func errMissingParameter(param string) error {
	return awserr.New("MissingParameter", fmt.Sprintf("missing required parameter %s", param), nil)
}

func errTableNotFound(tableName string) error {
	return awserr.New("ResourceNotFoundException", fmt.Sprintf("table %q not found", tableName), nil)
}

// Record of a call to MockDynamo. Stores a string name of the API endpoint
// ("PutItem", "GetItem", etc), the input struct received, the output struct
// sent back (if any), and the error sent back. The input and output are stored
// in interface{} so you'll need to do type assertions to pull out meaningful
// values.
type mockDynamoCallRecord struct {
	operation string
	input     interface{}
	output    interface{}
	err       error
}

// MockDynamo mocks the DynamoDB API in memory. It only supports GetItem,
// PutItem, and ScanPages. It only supports the most simple filter expressions:
// they must be of the form <column> <operator> :<value>, and operator must be
// =, <, <=, >, >=, or <>.
type MockDynamo struct {
	dynamodbiface.DynamoDBAPI

	// Stored data
	tables map[string][]mockDynamoItem

	// Diagnostic tools
	requests []mockDynamoCallRecord
}

// NewMockDynamo gets a dynamo interface for testing
func NewMockDynamo(tables []string) dynamodbiface.DynamoDBAPI {
	d := &MockDynamo{
		tables:   make(map[string][]mockDynamoItem),
		requests: make([]mockDynamoCallRecord, 0),
	}
	for _, t := range tables {
		d.addTable(t)
	}
	return d
}

func (d *MockDynamo) addTable(name string) {
	d.tables[name] = make([]mockDynamoItem, 0)
}

func (d *MockDynamo) deleteTable(name string) {
	delete(d.tables, name)
}

func (d *MockDynamo) recordCall(operation string, in, out interface{}, err error) {
	d.requests = append(d.requests, mockDynamoCallRecord{
		operation: operation,
		input:     in,
		output:    out,
		err:       err,
	})
}

// PutItem mocks the dynamo PutItem method
func (d *MockDynamo) PutItem(in *dynamodb.PutItemInput) (out *dynamodb.PutItemOutput, err error) {
	defer d.recordCall("PutItem", in, out, err)
	if in.TableName == nil {
		return nil, errMissingParameter("TableName")
	}
	if in.Item == nil {
		return nil, errMissingParameter("Item")
	}

	if aws.StringValue(in.TableName) == mockDynamoErrorTrigger {
		return nil, errInternalError()
	}

	tableName := aws.StringValue(in.TableName)
	if _, ok := d.tables[tableName]; !ok {
		return nil, errTableNotFound(tableName)
	}

	d.tables[tableName] = append(d.tables[tableName], in.Item)
	return &dynamodb.PutItemOutput{}, nil
}

// UpdateItem mocks the dynamo UpdateItem method
func (d *MockDynamo) UpdateItem(in *dynamodb.UpdateItemInput) (out *dynamodb.UpdateItemOutput, err error) {
	defer d.recordCall("UpdateItem", in, out, err)
	if in.TableName == nil {
		return nil, errMissingParameter("TableName")
	}
	if in.Key == nil {
		return nil, errMissingParameter("Key")
	}

	return &dynamodb.UpdateItemOutput{}, nil
}

// GetItem mocks the dynamo GetItem method
func (d *MockDynamo) GetItem(in *dynamodb.GetItemInput) (out *dynamodb.GetItemOutput, err error) {
	defer d.recordCall("GetItem", in, out, err)

	if in.TableName == nil {
		return nil, errMissingParameter("TableName")
	}
	if in.Key == nil {
		return nil, errMissingParameter("Key")
	}
	if aws.StringValue(in.TableName) == mockDynamoErrorTrigger {
		return nil, errInternalError()
	}

	tableName := aws.StringValue(in.TableName)
	if _, ok := d.tables[tableName]; !ok {
		return nil, errTableNotFound(tableName)
	}

	var filters []dynamoFilter
	for col, operand := range in.Key {
		filters = append(filters, dynamoFilter{
			col:     col,
			comp:    attrEqual,
			operand: operand,
		})
	}

	var match map[string]*dynamodb.AttributeValue
ItemLoop:
	for _, item := range d.tables[tableName] {
		for _, filter := range filters {
			if !item.applyFilter(filter) {
				continue ItemLoop
			}
		}
		match = item
		break
	}

	return &dynamodb.GetItemOutput{Item: match}, nil
}

// ScanPages mocks the dynamo ScanPages method
func (d *MockDynamo) ScanPages(in *dynamodb.ScanInput, pager func(*dynamodb.ScanOutput, bool) bool) (err error) {
	defer d.recordCall("ScanPages", in, nil, err)

	if in.TableName == nil {
		return errMissingParameter("TableName")
	}
	if aws.StringValue(in.TableName) == mockDynamoErrorTrigger {
		return errInternalError()
	}

	filter, err := parseFilter(aws.StringValue(in.FilterExpression), in.ExpressionAttributeValues)
	if err != nil {
		return err
	}

	table, ok := d.tables[aws.StringValue(in.TableName)]
	if !ok {
		return errTableNotFound(aws.StringValue(in.TableName))
	}

	var items []mockDynamoItem
	for _, item := range table {
		if item.applyFilter(filter) {
			items = append(items, item)
		}
	}

	var pages []*dynamodb.ScanOutput
	for i := 0; i < len(items); i += mockDynamoPageSize {
		end := i + mockDynamoPageSize
		if end > len(items) {
			end = len(items)
		}
		pageItems := make([]map[string]*dynamodb.AttributeValue, end-i)
		for j := range pageItems {
			pageItems[j] = (map[string]*dynamodb.AttributeValue)(items[i+j])
		}

		page := &dynamodb.ScanOutput{
			Count: aws.Int64(int64(end - i)),
			Items: pageItems,
		}
		pages = append(pages, page)
	}

	for i, p := range pages {
		if !pager(p, i == len(pages)-1) {
			break
		}
	}
	return nil
}

type mockDynamoItem map[string]*dynamodb.AttributeValue

type attrType int

const (
	unknownAttr attrType = iota
	binaryAttr
	boolAttr
	binarySetAttr
	listAttr
	mapAttr
	numberAttr
	numberSetAttr
	nullAttr
	stringAttr
	stringSetAttr
)

func typeOfAttr(v *dynamodb.AttributeValue) attrType {
	switch {
	case v.B != nil:
		return binaryAttr
	case v.BOOL != nil:
		return boolAttr
	case v.BS != nil:
		return binarySetAttr
	case v.L != nil:
		return listAttr
	case v.M != nil:
		return mapAttr
	case v.N != nil:
		return numberAttr
	case v.NS != nil:
		return numberSetAttr
	case v.NULL != nil:
		return nullAttr
	case v.S != nil:
		return stringAttr
	case v.SS != nil:
		return stringSetAttr
	default:
		return unknownAttr
	}
}

func attrEqual(l, r *dynamodb.AttributeValue) bool {
	if typeOfAttr(l) != typeOfAttr(r) {
		return false
	}

	// value equality
	if !bytes.Equal(l.B, r.B) ||
		aws.BoolValue(l.BOOL) != aws.BoolValue(r.BOOL) ||
		aws.BoolValue(l.NULL) != aws.BoolValue(r.NULL) ||
		parseNum(l.N) != parseNum(r.N) ||
		aws.StringValue(l.S) != aws.StringValue(r.S) {
		return false
	}

	// list equality
	if l.L != nil {
		if len(l.L) != len(r.L) {
			return false
		}
		for i, lv := range l.L {
			if !attrEqual(lv, r.L[i]) {
				return false
			}
		}
	}

	// map equality
	if l.M != nil {
		if len(l.M) != len(r.M) {
			return false
		}
		for k, lv := range l.M {
			if !attrEqual(lv, r.M[k]) {
				return false
			}
		}
	}

	// binary set equality
	if l.BS != nil {
		if len(l.BS) != len(r.BS) {
			return false
		}
		lSet := make(map[string]struct{})
		for _, k := range l.BS {
			lSet[string(k)] = struct{}{}
		}
		for _, k := range r.BS {
			if _, ok := lSet[string(k)]; !ok {
				return false
			}
		}
	}

	// number set equality
	if l.NS != nil {
		if len(l.NS) != len(r.NS) {
			return false
		}
		lSet := make(map[float64]struct{})
		for _, k := range l.NS {
			lSet[parseNum(k)] = struct{}{}
		}
		for _, k := range r.NS {
			if _, ok := lSet[parseNum(k)]; !ok {
				return false
			}
		}
	}

	// string set equality
	if l.SS != nil {
		if len(l.SS) != len(r.SS) {
			return false
		}
		lSet := make(map[string]struct{})
		for _, k := range l.SS {
			lSet[aws.StringValue(k)] = struct{}{}
		}
		for _, k := range r.SS {
			if _, ok := lSet[aws.StringValue(k)]; !ok {
				return false
			}
		}
	}

	return true
}

func attrNotEqual(l, r *dynamodb.AttributeValue) bool {
	return !attrEqual(l, r)
}

func attrLessThan(l, r *dynamodb.AttributeValue) bool {
	if typeOfAttr(l) != typeOfAttr(r) {
		return false
	}

	switch typeOfAttr(l) {
	case stringAttr:
		return aws.StringValue(l.S) < aws.StringValue(r.S)
	case numberAttr:
		return parseNum(l.N) < parseNum(r.N)
	default:
		return false
	}
}

func attrGreaterThan(l, r *dynamodb.AttributeValue) bool {
	return attrLessThan(r, l)
}

func attrLessThanOrEqual(l, r *dynamodb.AttributeValue) bool {
	return !attrLessThan(r, l)
}

func attrGreaterThanOrEqual(l, r *dynamodb.AttributeValue) bool {
	return !attrLessThan(l, r)
}

func parseNum(raw *string) float64 {
	if raw == nil {
		return 0
	}
	n, err := strconv.ParseFloat(*raw, 64)
	if err != nil {
		panic(err)
	}
	return n
}

type dynamoFilter struct {
	col     string
	comp    func(l, r *dynamodb.AttributeValue) bool
	operand *dynamodb.AttributeValue
}

// Parse a filter expression. Compound filter expressions with multiple
// conditions aren't supported. Only filters that look like
// 'column <comparator> :value' work.
func parseFilter(expr string, attrs map[string]*dynamodb.AttributeValue) (dynamoFilter, error) {
	out := dynamoFilter{}
	if len(expr) == 0 {
		out.comp = func(_, _ *dynamodb.AttributeValue) bool { return true }
		return out, nil
	}

	// parse out column
	splitExpr := strings.Split(expr, " ")
	if len(splitExpr) != 3 {
		return out, fmt.Errorf("unparseable filter, expected 'column cmp :val'. expr=%q", expr)
	}

	var rawComp, rawVal string
	out.col, rawComp, rawVal = splitExpr[0], splitExpr[1], splitExpr[2]

	// parse comparator
	switch rawComp {
	case "=":
		out.comp = attrEqual
	case "<>":
		out.comp = attrNotEqual
	case "<":
		out.comp = attrLessThan
	case "<=":
		out.comp = attrLessThanOrEqual
	case ">":
		out.comp = attrGreaterThan
	case ">=":
		out.comp = attrGreaterThanOrEqual
	default:
		return out, fmt.Errorf("unknown comparator %q", rawComp)
	}

	// parse operand
	if !strings.HasPrefix(rawVal, ":") {
		return out, fmt.Errorf("unparseable filter, expected 'column cmp :val' style. expr=%q", expr)
	}
	var ok bool
	out.operand, ok = attrs[rawVal]
	if !ok {
		return out, fmt.Errorf("missing filter argument %q", rawVal)
	}
	return out, nil
}

func (i mockDynamoItem) applyFilter(f dynamoFilter) bool {
	// Special case: an empty string is a filter which always returns true
	if f.col == "" {
		return true
	}

	itemVal, ok := i[f.col]
	if !ok {
		return false
	}

	if typeOfAttr(itemVal) != typeOfAttr(f.operand) {
		return false
	}

	return f.comp(itemVal, f.operand)

}

// AssertNoRequestsMade will Execute a function, asserting that no requests
// are made over the course of the function.
func AssertNoRequestsMade(t *testing.T, mock *MockDynamo, msg string, f func()) {
	nStart := len(mock.requests)
	f()
	nEnd := len(mock.requests)
	if nEnd > nStart {
		for i := nStart; i < nEnd; i++ {
			t.Errorf("%s: unexpected %s request made to dynamo", msg, mock.requests[i].operation)
		}
	}
}

// AssertRequestMade will Execute a function, asserting that at least one request
// is made over the course of the function.
func AssertRequestMade(t *testing.T, mock *MockDynamo, msg string, f func()) {
	nStart := len(mock.requests)
	f()
	nEnd := len(mock.requests)
	if nEnd == nStart {
		t.Errorf("%s: expected a call to be made to dynamo, but didn't see one", msg)
	}
}
