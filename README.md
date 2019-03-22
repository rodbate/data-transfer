Data Transfer Application

## 打包部署


#### 1. 开发环境
```
mvn clean package -Pdev

跳过测试： mvn clean package -Dmaven.test.skip=true -Pdev 
```
#### 2. 测试环境
```
mvn clean package -Ptest

跳过测试： mvn clean package -Dmaven.test.skip=true -Ptest
```
#### 3. 生产环境
```
mvn clean package -Pprod

跳过测试： mvn clean package -Dmaven.test.skip=true -Pprod
```