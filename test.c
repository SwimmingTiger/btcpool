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
    int stat = 0;
    char path[255] = {0};
    char buff[1024] = {0};
    int len;

    zh = zookeeper_init("127.0.0.1:2181", my_watcher, 10000, NULL, NULL, 0);

    printf("----------------- Create ----------------\n");
    stat = zoo_create(zh, "/jobmaker/lock", "hello", 6, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE, path, 255);
    printf("stat: %s -> %s\n", zerror(stat), path);

    stat = zoo_get(zh, path, 0, buff, &len, NULL);
    printf("stat: %s -> (%d) %s\n", zerror(stat), len, buff);

    getchar();

    zookeeper_close(zh);

    return 0;
}
