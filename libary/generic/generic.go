// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package generic

import "reflect"

// Zero 返回类型 T 的零值。
// 泛型函数 Zero 使用了泛型类型 T，T 可以是任何类型（使用了 `any` 作为约束，`any` 是 Go 1.18 引入的泛型新特性）。
// 该函数内部通过声明局部变量 zero T 来生成类型 T 的零值。
// 在 Go 中，零值是指：
//   - 对于数值类型，零值是 0。
//   - 对于布尔类型，零值是 false。
//   - 对于字符串类型，零值是空字符串 ""。
//   - 对于指针、切片、映射、通道、接口等引用类型，零值是 nil。
func Zero[T any]() T {
	// 声明一个类型为 T 的局部变量 zero。
	// 由于没有对其进行显式初始化，它会被赋予类型 T 的零值。
	var zero T
	// 返回该零值。
	return zero
}

// IsZero 判断传入的值 v 是否为其类型的零值。
// 该函数也是泛型的，接受任意类型 T 的值作为参数。
// 使用了 reflect 包，它可以在运行时对类型和值进行检查。
// 通过 reflect.ValueOf(&v).Elem() 获取变量 v 的反射值（即其底层的值），然后调用 IsZero() 方法检查是否为零值。
func IsZero[T any](v T) bool {
	// reflect.ValueOf(&v) 获取 v 的地址，返回其反射对象。
	// .Elem() 获取地址指向的实际值。
	// IsZero() 是 reflect.Value 的方法，判断该值是否为零值。
	return reflect.ValueOf(&v).Elem().IsZero()
}

// Equal 比较两个 `any` 类型的值是否相等。
// `any` 是 Go 的接口类型，表示任何类型（相当于 interface{}）。
// 通过内建的 `==` 操作符比较 v1 和 v2。
// 注意：`==` 操作符只能用于可以比较的类型。如果传入的两个值类型不支持 `==` 比较，编译时会报错（例如切片、映射、函数等类型不能直接比较）。
func Equal(v1, v2 any) bool {
	// 直接使用 == 操作符比较 v1 和 v2，返回比较结果。
	return v1 == v2
}
