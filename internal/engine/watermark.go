package engine

import (
	"fmt"
	"strings"
	"time"

	"github.com/Cikouyanqu/replicron/internal/model"
)

// watermarkToken is the placeholder the source query must reference when a
// task opts into incremental sync.
const watermarkToken = ":watermark"

// watermarkLiteral renders a tracked watermark value as a SQL literal for
// substitution into the source query. Times are normalized to UTC with a
// space separator and at most microsecond precision, which all four source
// dialects parse; strings are quoted with ” doubling.
func watermarkLiteral(v any) (string, error) {
	switch x := v.(type) {
	case time.Time:
		return "'" + x.UTC().Format("2006-01-02 15:04:05.999999") + "'", nil
	case int:
		return fmt.Sprintf("%d", x), nil
	case int64:
		return fmt.Sprintf("%d", x), nil
	case float64:
		return fmt.Sprintf("%g", x), nil
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'", nil
	default:
		return "", fmt.Errorf("unsupported watermark value type %T", v)
	}
}

// watermarkGreater reports whether a > b. Values of different dynamic kinds
// mean the source column type is unstable, which fails the run rather than
// silently mis-tracking the window.
func watermarkGreater(a, b any) (bool, error) {
	switch av := a.(type) {
	case time.Time:
		bv, ok := b.(time.Time)
		if !ok {
			return false, kindMismatch(a, b)
		}
		return av.After(bv), nil
	case string:
		bv, ok := b.(string)
		if !ok {
			return false, kindMismatch(a, b)
		}
		return av > bv, nil
	default:
		ai, aok := asInt64(a)
		bi, bok := asInt64(b)
		if aok && bok {
			return ai > bi, nil
		}
		af, aokf := asFloat64(a)
		bf, bokf := asFloat64(b)
		if aokf && bokf {
			return af > bf, nil
		}
		return false, kindMismatch(a, b)
	}
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	}
	return 0, false
}

func asFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

func kindMismatch(a, b any) error {
	return fmt.Errorf("incomparable values %T(%v) and %T(%v): watermark column type must be stable", a, a, b, b)
}

// watermarkTracker folds the running maximum of the incremental column over
// the rows streamed in the current run. NULL values are skipped.
type watermarkTracker struct {
	column string
	max    any
	seen   bool
}

func (w *watermarkTracker) observe(rows []model.Row) error {
	for _, r := range rows {
		v := r[w.column]
		if v == nil {
			continue
		}
		if !w.seen {
			w.max, w.seen = v, true
			continue
		}
		gt, err := watermarkGreater(v, w.max)
		if err != nil {
			return err
		}
		if gt {
			w.max = v
		}
	}
	return nil
}
