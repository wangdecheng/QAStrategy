import numpy as np
import matplotlib.pyplot as plt
import h5py
import skimage.transform as tf


def load_dataset():
    train_dataset = h5py.File('./datasets/train_catvnoncat.h5', "r")
    train_set_x_orig = np.array(train_dataset["train_set_x"][:])
    train_set_y_orig = np.array(train_dataset["train_set_y"][:])

    test_dataset = h5py.File('./datasets/test_catvnoncat.h5', "r")
    test_set_x_orig = np.array(test_dataset["test_set_x"][:])
    test_set_y_orig = np.array(test_dataset["test_set_y"][:])

    classes = np.array(test_dataset["list_classes"][:])

    train_set_y_orig = train_set_y_orig.reshape((1, train_set_y_orig.shape[0]))
    test_set_y_orig = test_set_y_orig.reshape((1, test_set_y_orig.shape[0]))

    return train_set_x_orig, train_set_y_orig, test_set_x_orig, test_set_y_orig, classes

def sigmoid(z):
    """
       参数:
       z -- 一个数值或者一个numpy数组.
       返回值:
       s -- 经过sigmoid算法计算后的值，在[0,1]范围内
       """
    s = 1 / (1 + np.exp(-z))
    return s

#初始化w,b的值
def initialize_with_zeros(dim):
    w = np.zeros((dim,1))
    b = 0
    return w,b


# 计算梯度，成本
def propagate(w, b, X, Y):
    """
    参数:
    w -- 权重数组，维度是(12288, 1)
    b -- 偏置bias
    X -- 图片的特征数据，维度是 (12288, 209)
    Y -- 图片对应的标签，0或1，0是无猫，1是有猫，维度是(1,209)

    返回值:
    cost -- 成本
    dw -- w的梯度
    db -- b的梯度
    """
    m = X.shape[1]
    # 前向传播
    A = sigmoid(np.dot(w.T, X) + b)
    cost = -np.sum(Y * np.log(A)) + (1 - Y) * np.log(1 - A) / m

    # 反向传播
    dz = A - Y  # z:预测值，dz:cost对z的偏导
    dw = np.dot(X, dz.T) / m
    db = np.sum(dz) / m

    grads = {"dw": dw, "db": db}

    return grads, cost


def optimize(w, b, X, Y, num_iterations, learning_rate, print_cost=False):
    """
    参数优化:
    w -- 权重数组，维度是 (12288, 1)
    b -- 偏置bias
    X -- 图片的特征数据，维度是 (12288, 209)
    Y -- 图片对应的标签，0或1，0是无猫，1是有猫，维度是(1,209)
    num_iterations -- 指定要优化多少次
    learning_rate -- 学习步进，是我们用来控制优化步进的参数
    print_cost -- 为True时，每优化100次就把成本cost打印出来,以便我们观察成本的变化

    返回值:
    params -- 优化后的w和b
    costs -- 每优化100次，将成本记录下来，成本越小，表示参数越优化
    """
    costs = []
    for i in range(num_iterations):
        grads, cost = propagate(w, b, X, Y)
        dw = grads["dw"]
        db = grads["db"]

        # 进行梯度下降
        w = w - learning_rate * dw
        b = b - learning_rate * db

        if i % 100 == 0:
            costs.append(cost)
            if print_cost:
                print("优化%i次后的成本是：%f", i, cost)

    params = {"w": w, "b": b}
    return params, costs


def predict(w, b, X):
    '''
    参数:
    w -- 权重数组，维度是 (12288, 1)
    b -- 偏置bias
    X -- 图片的特征数据，维度是 (12288, 图片张数)

    返回值:
    Y_prediction -- 对每张图片的预测结果
    '''
    m = X.shape[1]
    Y_prediction = np.zeros((1, m))
    A = sigmoid(np.dot(w.T, X) + b)

    for i in range(A.shape[1]):
        if A[0, i] >= 0.5:
            Y_prediction[0, i] = 1

    return Y_prediction


def model(X_train, Y_train, X_test, Y_test, num_iterations=2000, learning_rate=0.5, print_cost=False):
    """
    参数:
    X_train -- 训练图片,维度是(12288, 209)
    Y_train -- 训练图片对应的标签,维度是 (1, 209)
    X_test -- 测试图片,维度是(12288, 50)
    Y_test -- 测试图片对应的标签,维度是 (1, 50)
    num_iterations -- 需要训练/优化多少次
    learning_rate -- 学习步进，是我们用来控制优化步进的参数
    print_cost -- 为True时，每优化100次就把成本cost打印出来,以便我们观察成本的变化

    返回值:
    d -- 返回一些信息
    """
    w, b = initialize_with_zeros(X_train.shape[0])

    parameters, costs = optimize(w, b, X_train, Y_train, num_iterations, learning_rate, print_cost)

    w = parameters["w"]
    b = parameters["b"]

    Y_predication_train = predict(w, b, X_train)
    Y_predication_test = predict(w, b, X_test)

    print("对训练图片的预测准确率为：{}%".format(100 - np.mean(np.abs(Y_predication_train - Y_train)) * 100))
    print("对测试图片的预测准确率为：{}%".format(100 - np.mean(np.abs(Y_predication_test - Y_test)) * 100))

    d = {"costs": costs,
         "Y_prediction_test": Y_predication_test,
         "Y_prediction_train": Y_predication_train,
         "w": w,
         "b": b,
         "learning_rate": learning_rate,
         "num_iterations": num_iterations}

    return d

if __name__ == '__main__':
    train_set_x_orig, train_set_y, test_set_x_orig, test_set_y, classes = load_dataset()
    m_train = train_set_x_orig.shape[0]  # 训练样本数
    m_test = test_set_x_orig.shape[0]  # 测 试样本数
    num_px = test_set_x_orig.shape[1]  # 每张图片宽高，正方形
    train_set_x_flatten = train_set_x_orig.reshape(train_set_x_orig.shape[0], -1).T
    test_set_x_flatten = test_set_x_orig.reshape(test_set_x_orig.shape[0], -1).T

    print("train_set_x_flatten.shape:", train_set_x_flatten.shape)
    print("test_set_x_flatten.shape:", test_set_x_flatten.shape)

    # 调用上面的模型函数对我们最开始加载的数据进行训练
    d = model(train_set_x_flatten, train_set_y, test_set_x_flatten, test_set_y, num_iterations=20,
              learning_rate=0.005)
    #print("d",d)

    my_image = "my_image1.jpg"
    fname = "./images/" + my_image

    image = np.array(plt.imread(fname))
    my_image = tf.resize(image, (num_px, num_px), mode='reflect').reshape((1, num_px * num_px * 3)).T
    my_prediceted_image = predict(d["w"], d["b"],my_image)
    plt.imshow(image)
    print("预测结果 " + str(int(np.squeeze(my_prediceted_image))))
