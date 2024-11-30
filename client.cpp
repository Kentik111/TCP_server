#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <stdexcept>

using namespace std;

const int PORT = 7435;
const char* SERVER_IP = "127.0.0.1";  // IP-адрес сервера

class SocketRAII {
public:
    SocketRAII(int fd) : fd_(fd) {}
    ~SocketRAII() {
        if (close(fd_) < 0) {
            cerr << "Ошибка при закрытии сокета" << endl;
        }
    }
    int get() const { return fd_; }
private:
    int fd_;
};

void start_client() {
    SocketRAII client_socket(socket(AF_INET, SOCK_STREAM, 0));
    if (client_socket.get() < 0) {
        throw runtime_error("Ошибка создания сокета");
    }

    struct sockaddr_in server_address;
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(PORT);

    if (inet_pton(AF_INET, SERVER_IP, &server_address.sin_addr) <= 0) {
        throw runtime_error("Ошибка преобразования IP-адреса");
    }

    if (connect(client_socket.get(), (struct sockaddr*)&server_address, sizeof(server_address)) < 0) {
        throw runtime_error("Ошибка подключения к серверу");
    }

    cout << "Подключено к серверу " << SERVER_IP << ":" << PORT << endl;

    while (true) {
        string input;
        cout <<"\nSELECT <> FROM <> - выборка\nWHERE и операторы OR , AND - фильтрация\nINSERT INTO - вставка данных в таблицы\nDELETE FROM - удаление данных из таблицы\nEXIT-выход из программы\n";
        cout <<"------------------------------------\n"<<endl;
        getline(cin, input);

        if (input == "EXIT") {
            break;
        }

        send(client_socket.get(), input.c_str(), input.size(), 0);
        cout << "Запрос отправлен серверу." << endl;

        char buffer[1024] = {0};
        int bytesReceived = read(client_socket.get(), buffer, sizeof(buffer) - 1);
        if (bytesReceived < 0) {
            throw runtime_error("Ошибка при чтении данных от сервера");
        }

        string response(buffer, bytesReceived);
        cout << "Ответ от сервера: " << response << endl;
    }

    cout << "Отключение от сервера." << endl;
}

int main() {
    try {
        start_client();
    } catch (const exception& e) {
        cerr << "Ошибка: " << e.what() << endl;
        return -1;
    }

    return 0;
}
