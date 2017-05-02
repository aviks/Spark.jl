function init()
    envcp = get(ENV, "CLASSPATH", "")
    hadoopConfDir = get(ENV, "HADOOP_CONF_DIR", "")
    yarnConfDir = get(ENV, "YARN_CONF_DIR", "")
    defaults = load_spark_defaults()
    extracp = defaults["spark.driver.extraClassPath"]
    shome =  get(ENV, "SPARK_HOME", "")
    libassembly = joinpath(get(ENV, "SPARK_HOME", ""), "lib", "spark-assembly.jar")
    if isfile(libassembly)
        sparkjlassembly = libassembly
    else
        sparkjlassembly = joinpath(dirname(@__FILE__), "..", "jvm", "sparkjl", "target", "sparkjl-0.1-assembly.jar")
    end
    classpath = @static is_windows() ? "$envcp;$sparkjlassembly" : "$envcp:$sparkjlassembly"
    classpath = @static is_windows() ? "$classpath;$hadoopConfDir;$yarnConfDir;extracp" : "$classpath:$hadoopConfDir:$yarnConfDir:$extracp"

    try
        println("JVM starting from init.jl")
        # prevent exceptions in REPL on code reloading

        # JVM start in debug mode
        #JavaCall.init(["-ea", "-Xmx1024M", "-Djava.class.path=$classpath", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4000"])

        #JVM default start
        JavaCall.init(["-ea", "-Xmx1024M", "-cp $classpath", "-Djava.class.path=$classpath"])
    end
end

function load_spark_defaults()
    d=Dict()
    sconf = get(ENV, "SPARK_CONF", "")
    if sconf == ""
        shome =  get(ENV, "SPARK_HOME", "")
        if shome == "" ; return jconf; end
        sconf = joinpath(shome, "conf")
    end
    p = map(split, filter(isnotcomment, split(readstring(joinpath(sconf, "spark-defaults.conf")), '\n', keep=false) ) )
    for x in p
        d[x[1]] = x[2]
    end
    return d
end

function isnotcomment(x)
    return !startswith(x, "#")
end

init()
