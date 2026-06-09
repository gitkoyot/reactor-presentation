dependencies {
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-logging")

    // Reactor tools — ReactorDebugAgent: better stack traces in production (no perf cost)
    implementation("io.projectreactor:reactor-tools")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.junit.platform:junit-platform-launcher")

    // BlockHound — detects blocking calls on non-blocking threads
    testImplementation("io.projectreactor.tools:blockhound:1.0.17.RELEASE")
}

tasks.test {
    jvmArgs = listOf("-XX:+AllowRedefinitionToAddDeleteMethods")
}

