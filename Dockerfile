FROM bdtrotte/pistache:1.0

COPY . /usr/src/myApp
WORKDIR /usr/src/myApp
RUN g++ -std=c++17 -o myApp main.cpp ParseServer.cpp VectorClock.cpp View.cpp Node.cpp \
    ShardScheme.cpp ShardSchemeUtilitySerialization.cpp ShardSchemeUtility.cpp \
    ParsingHelpers.cpp \
    -lpistache -pthread

EXPOSE 8080

CMD ["./myApp"]
