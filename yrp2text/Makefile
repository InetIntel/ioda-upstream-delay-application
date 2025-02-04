CXX = c++
CFLAGS = -Os --std=gnu++11 -Wall
LIBS = -lscamperfile
DEPS = ipaddress.hpp yarrpfile.hpp
OBJ = yrp2text.o

%.o: %.cpp $(DEPS)
	$(CXX) -c -o $@ $< $(CFLAGS)

yrp2text: $(OBJ)
	$(CXX) -o $@ $^ $(CFLAGS) $(LIBS)

clean:
	rm -f yrp2text yrp2text.o