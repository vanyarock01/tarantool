all: ./routerc.so

./routerc.so: ./so/router_api.o ./so/router_call.o ./so/router.o ./so/msgpuck.o ./so/hints.o
	gcc -shared -fPIC -g ./so/msgpuck.o ./so/hints.o ./so/router_api.o ./so/router.o ./so/router_call.o -o ./routerc.so

./so/msgpuck.o: ./so/msgpuck.c
	gcc -fPIC -c -g ./so/msgpuck.c -I./so -o ./so/msgpuck.o

./so/hints.o: ./so/hints.c
	gcc -fPIC -c -g ./so/hints.c -I./so -o ./so/hints.o

./so/router_call.o: ./so/router_call.c
	gcc -fPIC -c -g ./so/router_call.c -I./so -o ./so/router_call.o

./so/router.o: ./tntcxx/src/Buffer/Buffer.hpp ./tntcxx/src/Client/Connector.hpp ./tntcxx/src/Utils/Mempool.hpp ./so/Router.cpp
	g++ -std=c++17 -g -fPIC -c ./so/Router.cpp -o ./so/router.o

./so/router_api.o: ./so/router_api.c
	g++ -std=c++17 -g -fPIC -c ./so/router_api.c -o ./so/router_api.o

clean:
	rm routerc.so
	rm -rf so/*.o
