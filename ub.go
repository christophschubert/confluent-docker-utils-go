package main

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"

	"golang.org/x/sys/unix"
)

func ensure(envVar string) bool {
	_, found := os.LookupEnv(envVar)
	return found
}

func ensureAtLeastOne(envVars []string) bool {
	for _, envVar := range envVars {
		if ensure(envVar) {
			return true
		}
	}
	return false
}

func path(filePath string, operation string) bool {
	switch operation {

	case "readable":
		return unix.Access(filePath, unix.R_OK) == nil
	case "executable":
		info, err := os.Stat(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error checking executable status of file %s: %s", filePath, err)
			return false
		}
		return info.Mode()&0111 != 0 //check whether file is executable by anyone, use 0100 to check for execution rights for owner
	case "existence":
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return false
		}
		return true
	case "writable":
		return unix.Access(filePath, unix.W_OK) == nil
	default:
		fmt.Fprintf(os.Stderr, "Unknown operation %s", operation)
	}
	return false
}

func connectForever(address string, ch chan<- string) {
	for {
		_, err := net.Dial("tcp", address)
		if err == nil {
			ch <- "success"
			return
		}
	}
}

func waitForServer(address string, timeout time.Duration) bool {
	c1 := make(chan string, 1)
	go connectForever(address, c1)
	select {
	case <-c1:
		return true
	case <-time.After(timeout):
		return false
	}
}

func waitForHttp(urlString string, timeout time.Duration) bool {
	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}

	server := ""
	if strings.Contains(u.Host, ":") {
		server = u.Host
	} else {
		hostPortMap := map[string]string{"http": "80", "https": "443"}
		port, found := hostPortMap[u.Scheme]
		if !found {
			panic("No port specified and cannot infer port based on protocol (only http(s) supported).")
		}
		server = net.JoinHostPort(u.Host, port)
	}
	if !waitForServer(server, timeout) {
		return false
	}
	resp, err := http.Get(urlString)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error retrieving url")
		return false
	}
	if resp.StatusCode/100 != 2 {
		fmt.Fprintln(os.Stderr, resp.Status)
		return false
	}
	return true
}

func parseSecondsDuration(s string) time.Duration {
	secs, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return time.Duration(int64(secs) * int64(time.Second))
}

func renderTemplate(writer io.Writer, template template.Template) {
	template.Execute(writer, GetEnvironment())
}

func renderConfig(writer io.Writer, configSpec ConfigSpec) {
	writeConfig(writer, BuildProperties(configSpec, GetEnvironment()))
}

func renderConfigViaPrefix(writer io.Writer, envVarPrefix string) {
	// used, eg, for schema registry  and all admin-properties
	spec := ConfigSpec{
		Prefixes: map[string]bool{envVarPrefix: false},
		Excludes: []string{},
		Renamed:  map[string]string{},
		Defaults: map[string]string{},
	}
	config := BuildProperties(spec, GetEnvironment())
	writeConfig(writer, config)
}

// ConvertKey Converts an environment variable name to a property-name according to the following rules:
// - a single underscore (_) is replaced with a .
// - a double underscore (__) is replaced with a single underscore
// - a triple underscore (___) is replaced with a dash
// Moreover, the whole string is converted to lower-case.
// The behavior of sequences of four or more underscores is undefined.
func ConvertKey(key string) string {
	re := regexp.MustCompile("[^_]_[^_]")
	singleReplaced := re.ReplaceAllStringFunc(key, replaceUnderscores)
	singleTripleReplaced := strings.ReplaceAll(singleReplaced, "___", "-")
	return strings.ToLower(strings.ReplaceAll(singleTripleReplaced, "__", "_"))
}

//replaceUnderscores replaces every underscore '_' by a dot '.'
func replaceUnderscores(s string) string {
	return strings.ReplaceAll(s, "_", ".")
}

type ConfigSpec struct {
	Prefixes map[string]bool   `json:"prefixes"`
	Excludes []string          `json:"excludes"`
	Renamed  map[string]string `json:"renamed"`
	Defaults map[string]string `json:"defaults"`
}

//Contains returns true if slice contains element, and false otherwise.
func Contains(slice []string, element string) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}
	return false
}

//ListToMap splits each and entry of the kvList argument at '=' into a key/value pair and returns a map of all the k/v pair thus obtained.
func ListToMap(kvList []string) map[string]string {
	m := make(map[string]string)
	for _, l := range kvList {
		parts := strings.Split(l, "=")
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

func splitToMapDefaults(separator string, defaultValues string, value string) map[string]string {
	values := KvStringToMap(defaultValues, separator)
	for k, v := range KvStringToMap(value, separator) {
		values[k] = v
	}
	return values
}
func KvStringToMap(kvString string, sep string) map[string]string {
	return ListToMap(strings.Split(kvString, sep))
}

//GetEnvironment returns the current environment as a map.
func GetEnvironment() map[string]string {
	return ListToMap(os.Environ())
}

//BuildProperties creates a map suitable to be output as Java properties from a ConfigSpec and a map representing an environment.
func BuildProperties(spec ConfigSpec, environment map[string]string) map[string]string {
	config := make(map[string]string)
	for key, value := range spec.Defaults {
		config[key] = value
	}
	for envKey, envValue := range environment {
		if newKey, found := spec.Renamed[envKey]; found {
			config[newKey] = envValue
		} else {
			if !Contains(spec.Excludes, envKey) {
				for prefix, keep := range spec.Prefixes {
					if strings.HasPrefix(envKey, prefix) {
						var effectiveKey string
						if keep {
							effectiveKey = envKey
						} else {
							effectiveKey = envKey[len(prefix)+1:]
						}
						config[ConvertKey(effectiveKey)] = envValue
					}
				}
			}
		}
	}
	return config
}

func formatHeritage() string {
	return "# created by 'ub' from environment variables on " + time.Now().String()
}

func writeConfig(writer io.Writer, config map[string]string) {
	_, err := fmt.Fprintln(writer, formatHeritage())
	if err != nil {
		panic(err)
	}
	// Go randomizes iterations over map by design. We sort properties by name to ease debugging:
	sortedNames := make([]string, 0, len(config))
	for name := range config {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)
	for _, n := range sortedNames {
		_, err := fmt.Fprintf(writer, "%s=%s\n", n, config[n])
		if err != nil {
			panic(err)
		}
	}
}

func listenersFromAdvertisedListeners(listeners string) string {
	re := regexp.MustCompile("://(.*?):")
	return re.ReplaceAllString(listeners, "://0.0.0.0:")
}

func loadConfigSpec(path string) ConfigSpec {
	jsonFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	bytes, err := io.ReadAll(jsonFile)
	if err != nil {
		panic(err)
	}

	var spec ConfigSpec

	errParse := json.Unmarshal(bytes, &spec)
	if errParse != nil {
		panic(errParse)
	}
	return spec
}

/*
TOOD: add remark about how flags work in golang
*/
func checkHttp(host string, port string, timeout time.Duration, path string, useHttps bool, ignoreCert bool, username string, password string, pred func(string) bool) bool {
	address := host + ":" + port
	if !waitForServer(host+":"+port, timeout) {
		fmt.Fprintf(os.Stderr, "Could not reach address %s in %s", address, timeout.String())
		return false
	}
	tlsConf := &tls.Config{
		InsecureSkipVerify: ignoreCert,
	}
	tr := &http.Transport{TLSClientConfig: tlsConf}
	client := &http.Client{Transport: tr, Timeout: timeout}
	url := ""
	if useHttps {
		url = "https://" + host + ":" + port
	} else {
		url = "http://" + host + ":" + port
	}
	if path != "" {
		url = url + "/" + path
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	if username != "" || password != "" {
		req.SetBasicAuth(username, password)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error performing request to %s", url)
		return false
	}
	if resp.StatusCode/100 != 2 {
		fmt.Fprintf(os.Stderr, "Failed to perform, %d", resp.StatusCode)
		return false
	}
	if pred != nil {
		bodyText, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error reading response")
			return false
		}
		return pred(string(bodyText))
	}
	return true
}

func performHttpCheck(path string, pred func(string) bool) bool {
	httpReadyCmd := flag.NewFlagSet("sr-ready", flag.ExitOnError)
	httpReadySecure := httpReadyCmd.Bool("secure", false, "Use TLS to secure the connection")
	httpReadyIngnoreCert := httpReadyCmd.Bool("ignore_cert", false, "Ignore TLS certificate errors")
	httpReadyUserName := httpReadyCmd.String("username", "", "Username used to authenticate to the Schema Registry")
	httpReadyPassword := httpReadyCmd.String("password", "", "Password used to authenticate to the Schema Registry")

	httpReadyCmd.Parse(os.Args[2:])
	if httpReadyCmd.NArg() != 3 {
		fmt.Fprint(os.Stderr, "Missing positional argument: ")
		fmt.Fprintln(os.Stderr, httpReadyCmd.Args())
		return false
	} else {
		return checkHttp(httpReadyCmd.Arg(0), httpReadyCmd.Arg(1), parseSecondsDuration(httpReadyCmd.Arg(2)), path, *httpReadySecure, *httpReadyIngnoreCert, *httpReadyUserName, *httpReadyPassword,
			pred)
	}
}

func invokeJavaCommand(className string, jvmOpts string, args []string) bool {
	//TODO: change to system-path
	classPath := getEnvOrDefault("CUB_CLASSPATH", "/Users/cschubert/git/christophschubert/confluent-docker-utils-go/deps.jar")

	opts := []string{}
	if jvmOpts != "" {
		opts = append(opts, jvmOpts)
	}
	opts = append(opts, "-cp", classPath, className)
	cmd := exec.Command("java", append(opts[:], args...)...)

	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			return exitError.ExitCode() == 0
		}
		return false
	}
	return true
}

func getEnvOrDefault(envVar string, defaultValue string) string {
	v, found := os.LookupEnv(envVar)
	if !found {
		return defaultValue
	}
	return v
}

func checkKafkaReady(minNumBroker string, timeout string, bootstrapServers string, zookeeperConnect string, configFile string, security string) bool {

	opts := []string{minNumBroker, timeout + "000"}
	if bootstrapServers != "" {
		opts = append(opts, "-b", bootstrapServers)
	}
	if zookeeperConnect != "" {
		opts = append(opts, "-z", zookeeperConnect)
	}
	if configFile != "" {
		opts = append(opts, "-c", configFile)
	}
	if security != "" {
		opts = append(opts, "s", security)
	}
	jvmOpts := os.Getenv("KAFKA_OPTS")
	return invokeJavaCommand("io.confluent.admin.utils.cli.KafkaReadyCommand", jvmOpts, opts)
}

func ensureTopic(configFile string, topicConfigFile string, timeout string, createIfNotExists bool) bool {
	opts := []string{
		"--config", configFile,
		"--file", topicConfigFile,
		"--create-if-not-exists", "" + strconv.FormatBool(createIfNotExists),
		"--timeout", timeout + "000",
	}
	return invokeJavaCommand(
		"io.confluent.kafkaensure.cli.TopicEnsureCommand",
		os.Getenv("KAFKA_OPTS"),
		opts,
	)
}

func waitForPathForever(pathToWaitFor string, ch chan<- string) {
	for {
		if path(pathToWaitFor, "existence") {
			ch <- "success"
		}
		time.Sleep(time.Second)
	}
}

func waitForPath(path string, timeoutSeconds string) bool {
	//TODO: refactor to use parseSecondsDuration instead of passing in a string
	timeout, err := time.ParseDuration(timeoutSeconds + "s")
	if err != nil {
		panic(err)
	}
	c1 := make(chan string, 1)
	go waitForPathForever(path, c1)
	select {
	case <-c1:
		return true
	case <-time.After(timeout):
		return false
	}
}

func checkAndPrintUsage(numArguments int, message string) {
	if len(os.Args) != numArguments {
		fmt.Fprintf(os.Stderr, "Usage '%s %s %s", os.Args[0], os.Args[1], message)
		os.Exit(1)
	}
}

func main() {
	success := false
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage '%s <subcommand> ...'", os.Args[0])
		os.Exit(1)
	}
	switch os.Args[1] {
	//commands from the 'dub' tool
	case "template":
		fmt.Fprintln(os.Stderr, "templating no longer supported, use 'render-template', 'render-properties', or 'render-properties-prefix' instead")
		os.Exit(2)
	case "render-template":
		// render a template (used for log4j properties)
		checkAndPrintUsage(3, "<path-to-template>")
		templateFile, err := os.Open(os.Args[2])
		if err != nil {
			panic(err) // TODO: write to stderr instead of break
		}
		bytes, err := io.ReadAll(templateFile)
		if err != nil {
			panic(err)
		}
		funcs := template.FuncMap{
			"formatHeritage":     formatHeritage,
			"getEnv":             getEnvOrDefault,
			"split":              strings.Split,
			"splitToMapDefaults": splitToMapDefaults,
		}
		t := template.Must(template.New("tmpl").Funcs(funcs).Parse(string(bytes)))
		renderTemplate(os.Stdout, *t)
		success = true
	case "render-properties":
		checkAndPrintUsage(3, "<path-to-config-spec>")
		configSpec := loadConfigSpec(os.Args[2])
		renderConfig(os.Stdout, configSpec)
		success = true
	case "render-properties-prefix":
		checkAndPrintUsage(3, "<env-var-prefix>")
		renderConfigViaPrefix(os.Stdout, os.Args[2])
		success = true
	case "ensure":
		success = ensure(os.Args[2])
	case "ensure-atleast-one":
		success = ensureAtLeastOne(os.Args[2:])
	case "wait":
		success = waitForServer(os.Args[2], parseSecondsDuration(os.Args[3]))
	case "http-ready":
		success = waitForHttp(os.Args[2], parseSecondsDuration(os.Args[3]))
	case "path":
		success = path(os.Args[2], os.Args[3])
	case "path-wait":
		success = waitForPath(os.Args[2], os.Args[3])

	case "listeners":
		println(listenersFromAdvertisedListeners(os.Args[2]))
		success = true
	case "ensure-topic":
		ensureTopicCmd := flag.NewFlagSet("ensure-topic", flag.ExitOnError)
		ensureTopicCreate := ensureTopicCmd.Bool("create_if_not_exists", false, "Create topics if they do not yet exist.")
		ensureTopicCmd.Parse(os.Args[2:])
		if ensureTopicCmd.NArg() != 3 {
			fmt.Fprintln(os.Stderr, "Missing positional argument", ensureTopicCmd.Args())
		} else {
			success = ensureTopic(ensureTopicCmd.Arg(0), ensureTopicCmd.Arg(1), ensureTopicCmd.Arg(2), *ensureTopicCreate)
		}
	case "kafka-ready":
		//first positional argument: number brokers
		//second positional argument: timeout in seconds
		kafkaReadyCmd := flag.NewFlagSet("kafka-ready", flag.ExitOnError)
		kafkaReadyBootstrap := kafkaReadyCmd.String("b", "", "Bootstrap broker list")
		kafkaReadyZooKeeper := kafkaReadyCmd.String("z", "", "ZooKeeper connect string")
		kafkaReadyConfig := kafkaReadyCmd.String("c", "", "Path to config properties")
		kafkaReadySecurity := kafkaReadyCmd.String("s", "", "Security protocol")

		kafkaReadyCmd.Parse(os.Args[2:])
		if kafkaReadyCmd.NArg() != 2 {
			fmt.Fprintln(os.Stderr, "Missing positional argument", kafkaReadyCmd.Args())
		} else {
			success = checkKafkaReady(kafkaReadyCmd.Arg(0), kafkaReadyCmd.Arg(1), *kafkaReadyBootstrap, *kafkaReadyZooKeeper, *kafkaReadyConfig, *kafkaReadySecurity)
		}
	case "zk-ready":
		checkAndPrintUsage(4, "<zookeeper-connect> <timeout-in-seconds>")

		jvmOpts := ""
		isZooKeeperSaslEnabled := getEnvOrDefault("ZOOKEEPER_SASL_ENABLED", "")
		if strings.ToUpper(isZooKeeperSaslEnabled) != "FALSE" {
			jvmOpts = os.Getenv("KAFKA_OPTS")
		}
		args := [...]string{os.Args[2], os.Args[3] + "000"}

		success = invokeJavaCommand("io.confluent.admin.utils.cli.ZookeeperReadyCommand", jvmOpts, args[:])

	case "sr-ready":
		success = performHttpCheck("config", func(s string) bool { return strings.Contains(s, "compatibilityLevel") })
	case "kr-ready":
		success = performHttpCheck("topics", nil)
	case "connect-ready":
		success = performHttpCheck("", func(s string) bool { return strings.Contains(s, "version") })
	case "ksql-server-ready":
		success = performHttpCheck("info", func(s string) bool { return strings.Contains(s, "Ksql") })
	case "control-center-ready":
		success = performHttpCheck("", func(s string) bool { return strings.Contains(s, "Control Center") })
	default:
		fmt.Fprintln(os.Stderr, "Unknown subcommand "+os.Args[1])
	}

	if !success {
		os.Exit(1)
	}
}
