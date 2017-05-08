global const SPARK_DEFAULT_PROPS = Dict()

function init()
    JavaCall.addClassPath(get(ENV, "CLASSPATH", ""))
    defaults = load_spark_defaults(SPARK_DEFAULT_PROPS)
    shome =  get(ENV, "SPARK_HOME", "")
    if !isempty(shome)
        for x in readdir(joinpath(shome, "jars"))
            JavaCall.addClassPath(joinpath(shome, "jars", x))
        end
        JavaCall.addClassPath(joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1.jar"))
    else
        JavaCall.addClassPath(joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1-assembly.jar"))
    end
    for x in readdir("/usr/lib/hdinsight-datalake/")
        JavaCall.addClassPath(joinpath("/usr/lib/hdinsight-datalake/", x))
    end

    for y in split(get(defaults, "spark.driver.extraClassPath", ""), " ", keep=false)
        JavaCall.addClassPath(y)
    end
    JavaCall.addClassPath(get(defaults, "spark.driver.extraClassPath", ""))
    JavaCall.addClassPath(get(ENV, "HADOOP_CONF_DIR", ""))
    JavaCall.addClassPath(get(ENV, "YARN_CONF_DIR", ""))
    if get(ENV, "HDP_VERSION", "") == ""
       ENV["HDP_VERSION"] = pipeline(`hdp-select status` , `grep spark2-client` , `awk -F " " '{print $3}'`) |> readstring |> strip
    end

    for y in split(get(defaults, "spark.driver.extraJavaOptions", ""), " ", keep=false)
        JavaCall.addOpts(String(y))
    end
    JavaCall.addOpts("-ea")
    JavaCall.addOpts("-Xmx1024M")
    try
        println("JVM starting from init.jl")
        # prevent exceptions in REPL on code reloading

        # JVM start in debug mode
        #JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4000"])

        #JVM default start
        JavaCall.init()
    end
end

function load_spark_defaults(d::Dict)
    sconf = get(ENV, "SPARK_CONF", "")
    if sconf == ""
        shome =  get(ENV, "SPARK_HOME", "")
        if shome == "" ; return jconf; end
        sconf = joinpath(shome, "conf")
    end
    p = split(readstring(joinpath(sconf, "spark-defaults.conf")), '\n', keep=false)
    for x in p
         if !startswith(x, "#") && !isempty(strip(x))
             y=split(x, " ", limit=2); println(y)
             d[y[1]]=y[2]
         end
    end
    return d
end

init()
