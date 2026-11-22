# lake-service 运行镜像：jar 在宿主机由 bin/lake-service.sh build 预先生成。
# 不在镜像里跑 Maven，避免把整个 reactor + Spark 依赖塞进查询服务镜像。
FROM eclipse-temurin:21-jre

WORKDIR /app

# 通配符只匹配可执行 jar；sources / .original 不会被带进来
COPY lake-service/target/lake-service-*.jar /app/lake-service.jar

# 配置全部由 compose 的 LAKE_* 环境变量提供，镜像里不放 conf：
# 避免把宿主机的本地配置和 S3 密钥打进镜像。
RUN mkdir -p /app/logs

EXPOSE 8088

# 内存交给容器 limit 控制；堆占 75% 是给元空间/直接内存留余量
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/lake-service.jar"]
