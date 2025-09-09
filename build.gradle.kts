plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.19.2")
    implementation("com.rabbitmq:amqp-client:5.26.0")
    implementation("ch.qos.logback:logback-classic:1.5.18")}

tasks.test {
    useJUnitPlatform()
}