namespace python_server {

const unsigned int shmemBlockSize = 512 * 1024;
const unsigned int maxScriptSize = shmemBlockSize - 1;

const unsigned short defaultPort = 5555;
const unsigned short defaultPyExecPort = defaultPort + 1;

const char *const shmemName = "PyExec";

} // namespace python_server
