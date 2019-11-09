#include <iostream>
#include <thread>
#include <algorithm>
#include <vector>

template <typename T>
class Intersect{
private:
    std::vector<T> a;
    std::vector<T> b;
    std::vector<T> r;
    void membership(int va)
    {
        for (auto vb: b)
            {
                if (va == vb)
                {
                    r.push_back(va);
                }
            }
    }
public:
    Intersect(std::vector<T> a, std::vector<T> b): a(a), b(b)
    {
        int length = a.size();
        if (b.size() < length)
        {
            a.swap(b);
            length = a.size();
        }
        std::thread threads[length];
        for (int i = 0; i < length; i++)
            threads[i] = std::thread(&Intersect::membership, this, std::ref(a[i]));
        for (int i = 0; i < length; i++)
            threads[i].join();
    }
    std::vector<T> operator()()
    {
        sort(r.begin(), r.end());
        return r;
    }
};

// Driver Code
int main()
{
    std::vector<int> a = { 1, 5, 7, 14, 15, 18, 20, 22, 25};
    std::vector<int> b = { 1, 5, 7, 10, 12, 14, 15, 18, 20, 22, 25, 27, 30, 64, 110, 220 };
    std::vector<int> r = Intersect<int>(a, b)();
    for(auto v: r)
        std::cout<<v<<std::endl;
	return 0;
}
