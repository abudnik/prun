/*
===========================================================================

This software is licensed under the Apache 2 license, quoted below.

Copyright (C) 2013 Andrey Budnik <budnik27@gmail.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.

===========================================================================
*/
#ifndef __COMMON_H
#define __COMMON_H

namespace python_server {

extern unsigned int SHMEM_BLOCK_SIZE;
extern unsigned int MAX_SCRIPT_SIZE;

extern unsigned short DEFAULT_PORT;
extern unsigned short DEFAULT_PYEXEC_PORT;

extern const char SHMEM_NAME[];
extern const char FIFO_NAME[];

extern const char NODE_SCRIPT_NAME[];

} // namespace python_server

#endif
