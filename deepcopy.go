// deepcopy.go — values stored in and read from the L1 cache are deep copies, so a caller that
// keeps mutating the struct it passed to Set (or got back from Get) can't change what other
// readers see, or race with them.

package strata

import "reflect"

// deepCopy returns a copy of v that shares no mutable memory with it: slices, maps and pointers
// reachable through exported fields are copied. Unexported fields are copied by value (a type
// like time.Time keeps its immutable *Location).
func deepCopy(v any) any {
	if v == nil {
		return nil
	}
	return copyValue(reflect.ValueOf(v)).Interface()
}

func copyValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type().Elem())
		out.Elem().Set(copyValue(v.Elem()))
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v) // everything, including unexported fields, by value
		for i := 0; i < v.NumField(); i++ {
			if f := out.Field(i); f.CanSet() && needsCopy(f.Kind()) {
				f.Set(copyValue(v.Field(i)))
			}
		}
		return out
	case reflect.Slice:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		if !needsCopy(v.Type().Elem().Kind()) {
			reflect.Copy(out, v)
			return out
		}
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(copyValue(v.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(copyValue(v.Index(i)))
		}
		return out
	case reflect.Map:
		if v.IsNil() {
			return v
		}
		out := reflect.MakeMapWithSize(v.Type(), v.Len())
		iter := v.MapRange()
		for iter.Next() {
			out.SetMapIndex(iter.Key(), copyValue(iter.Value()))
		}
		return out
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(copyValue(v.Elem()))
		return out
	default:
		return v
	}
}

func needsCopy(k reflect.Kind) bool {
	switch k {
	case reflect.Ptr, reflect.Struct, reflect.Slice, reflect.Array, reflect.Map, reflect.Interface:
		return true
	}
	return false
}
