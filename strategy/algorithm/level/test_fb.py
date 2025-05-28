import matplotlib.pyplot as plt


def fibonacci_sequence(n):
    sequence = [0, 1]
    while len(sequence) < n:
        sequence.append(sequence[-1] + sequence[-2])
    return sequence


if __name__ == '__main__':
    # 繪製斐波那契數列
    # n = 10
    # fib_sequence = fibonacci_sequence(n)
    # print(fib_sequence)
    plt.figure(figsize=(10, 6))
    fib_sequence1 = [1, 10, 2, 20, 3, 1]
    fib_sequence2 = [2, 5, 8, 13, 21, 34]
    # 繪製第一條線，顏色為藍色，名稱為 'Sequence 1'
    plt.plot(fib_sequence1, marker='o', linestyle='-',
             color='b', label='Sequence 1')
    # 繪製第二條線，顏色為紅色，名稱為 'Sequence 2'
    plt.plot(fib_sequence2, marker='s', linestyle='--',
             color='r', label='Sequence 2')
    plt.title('Fibonacci Sequence')
    plt.xlabel('Index')
    plt.ylabel('Fibonacci Number')
    plt.grid(True)
    plt.show()
