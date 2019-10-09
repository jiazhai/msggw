To build and test:
```
bazel test //...
```

To build a deployable jar:
```
bazel build //msggw:msggw_deploy.jar
```

The deployable jar is created as ```bazel-bin/msggw/msggw_deploy.jar```. To run:
```
java -cp conf:bazel-bin/msggw/msggw_deploy.jar io.streaml.msggw.MessagingGatewayStarter
```