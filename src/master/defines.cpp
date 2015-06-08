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

#include "defines.h"

namespace master {

unsigned short NODE_PORT = 5555;
unsigned short NODE_UDP_PORT = NODE_PORT - 1;
unsigned short MASTER_UDP_PORT = NODE_PORT - 2;
unsigned short MASTER_ADMIN_PORT = NODE_PORT + 2;
unsigned short MASTERDB_PORT = 5559;

char const GROUPS_FILE_NAME[] = "groups";

} // namespace master
