#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 调用 $base_dir/kafka-run-class.sh 脚本并传递相应的参数。其中 "@ 代表传递的为命令行参数。
# 具体执行的封装在 Kafka Core 包中的 TopicCommand 类。
exec $(dirname $0)/kafka-run-class.sh kafka.admin.TopicCommand "$@"
