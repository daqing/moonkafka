// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "daqing/moonkafka"

version = "0.2.0"

readme = "README.mbt.md"

repository = "https://github.com/daqing/moonkafka"

license = "MIT"

keywords = [ "Kafka", "streaming" ]

preferred_target = "native"

description = "Open-source Apache Kafka client driver written in pure MoonBit"

import {
  "moonbitlang/async@0.21.2",
  "moonbitlang/x@0.5.1",
}
