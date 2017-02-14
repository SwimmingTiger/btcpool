#include <zookeeper/zookeeper.h>
#include <zookeeper/proto.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

void my_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
    printf("watcher: %d %s\n", type, path);
}

int main() {
    zhandle_t *zh = NULL;

    zh = zookeeper_init("127.0.0.1:9092", my_watcher, 10, NULL, NULL, 0);

    return 0;
}
