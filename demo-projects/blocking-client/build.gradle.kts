dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-logging")

    // Pooled HTTP client for RestTemplate — the default HttpURLConnection keeps only
    // ~5 keep-alive connections per host, which causes a SYN storm at high concurrency
    implementation("org.apache.httpcomponents.client5:httpclient5")
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.junit.platform:junit-platform-launcher")
}
