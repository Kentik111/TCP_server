#ifndef LINKEDLIST_HPP
#define LINKEDLIST_HPP

#include <iostream>
#include <stdexcept>

template <typename T>
class LinkedList {
 private:
  struct Node {
    T data;
    Node* next;
    Node* prev;

    Node(const T& value) : data(value), next(nullptr), prev(nullptr) {}
  };

  Node* head;  // Указатель на начало списка
  Node* tail;  // Указатель на конец списка
  size_t size;  // Текущий размер (количество элементов)

 public:
  LinkedList();
  ~LinkedList();

  void push_back(const T& value);  // Добавление элемента в конец
  void insert_beginning(const T& value);  // Вставка элемента в начало
  T& operator[](size_t index);  // Оператор доступа по индексу
  const T& operator[](size_t index) const;
  size_t getSize() const;  // Получение текущего размера
  void clear();            // Очистка списка
  bool empty() const;      // Проверка на пустоту
  T& back();   // Возврат последнего элемента
  T& front();  // Возврат первого элемента
  void erase(size_t index);  // Удаление элемента по индексу
  LinkedList<T> copy() const;

  bool contains(const T& value) const;  // Проверка на наличие элемента

  void print() const;  // Вывод элементов списка

  // Итераторы
  class Iterator {
   public:
    Iterator(Node* ptr) : current(ptr) {}

    T& operator*() { return current->data; }
    Iterator& operator++() {
      current = current->next;
      return *this;
    }
    Iterator& operator--() {
      current = current->prev;
      return *this;
    }
    bool operator!=(const Iterator& other) const {
      return current != other.current;
    }
    bool operator==(const Iterator& other) const {
      return current == other.current;
    }

   private:
    Node* current;  // Указатель на текущий элемент
  };

  Iterator begin() { return Iterator(head); }
  Iterator end() { return Iterator(nullptr); }

  // Итераторы для константных объектов
  class ConstIterator {
   public:
    ConstIterator(const Node* ptr) : current(ptr) {}

    const T& operator*() const { return current->data; }
    ConstIterator& operator++() {
      current = current->next;
      return *this;
    }
    ConstIterator& operator--() {
      current = current->prev;
      return *this;
    }
    bool operator!=(const ConstIterator& other) const {
      return current != other.current;
    }
    bool operator==(const ConstIterator& other) const {
      return current == other.current;
    }

   private:
    const Node* current;  // Указатель на текущий элемент
  };

  ConstIterator begin() const { return ConstIterator(head); }
  ConstIterator end() const { return ConstIterator(nullptr); }
};

// Определения методов шаблонного класса
template <typename T>
LinkedList<T>::LinkedList() : head(nullptr), tail(nullptr), size(0) {}

template <typename T>
LinkedList<T>::~LinkedList() {}

template <typename T>
void LinkedList<T>::push_back(const T& value) {
  Node* newNode = new Node(value);
  if (tail) {
    tail->next = newNode;
    newNode->prev = tail;
    tail = newNode;
  } else {
    head = tail = newNode;
  }
  ++size;
}

template <typename T>
void LinkedList<T>::insert_beginning(const T& value) {
  Node* newNode = new Node(value);
  if (head) {
    newNode->next = head;
    head->prev = newNode;
    head = newNode;
  } else {
    head = tail = newNode;
  }
  ++size;
}

template <typename T>
T& LinkedList<T>::operator[](size_t index) {
  if (index >= size) throw std::out_of_range("Index out of range");

  Node* current = head;
  for (size_t i = 0; i < index; ++i) {
    current = current->next;
  }
  return current->data;
}

template <typename T>
const T& LinkedList<T>::operator[](size_t index) const {
  if (index >= size) throw std::out_of_range("Index out of range");

  Node* current = head;
  for (size_t i = 0; i < index; ++i) {
    current = current->next;
  }
  return current->data;
}

template <typename T>
size_t LinkedList<T>::getSize() const {
  return size;
}

template <typename T>
void LinkedList<T>::clear() {
  while (head) {
    Node* temp = head;
    head = head->next;
    delete temp;
  }
  head = tail = nullptr;
  size = 0;
}

template <typename T>
bool LinkedList<T>::empty() const {
  return size == 0;
}

template <typename T>
T& LinkedList<T>::back() {
  if (!tail) throw std::out_of_range("List is empty");
  return tail->data;
}

template <typename T>
T& LinkedList<T>::front() {
  if (!head) throw std::out_of_range("List is empty");
  return head->data;
}

template <typename T>
void LinkedList<T>::erase(size_t index) {
  if (index >= size) throw std::out_of_range("Index out of range");

  Node* current = head;
  for (size_t i = 0; i < index; ++i) {
    current = current->next;
  }

  if (current->prev)
    current->prev->next = current->next;
  else
    head = current->next;

  if (current->next)
    current->next->prev = current->prev;
  else
    tail = current->prev;

  delete current;
  --size;
}

template <typename T>
bool LinkedList<T>::contains(const T& value) const {
  Node* current = head;
  while (current) {
    if (current->data == value) return true;
    current = current->next;
  }
  return false;
}

template <typename T>
void LinkedList<T>::print() const {
  Node* current = head;
  while (current) {
    std::cout << current->data << " ";
    current = current->next;
  }
  std::cout << std::endl;
}

template <typename T>
LinkedList<T> LinkedList<T>::copy() const {
  LinkedList<T> newList;  // Новый объект LinkedList для копии
  Node* current = head;
  while (current) {
    newList.push_back(
        current->data);  // Добавляем каждый элемент в новый список
    current = current->next;
  }
  return newList;
}

#endif // LINKEDLIST_HPP