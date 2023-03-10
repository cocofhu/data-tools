package com.cocofhu.utils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

public class ReflectionUtils {
    private static final Set<Class<?>> BASIC_BOXING_TYPE;

    static {
        BASIC_BOXING_TYPE = new HashSet<>();
        BASIC_BOXING_TYPE.add(Integer.class);
        BASIC_BOXING_TYPE.add(Double.class);
        BASIC_BOXING_TYPE.add(Float.class);
        BASIC_BOXING_TYPE.add(Boolean.class);
        BASIC_BOXING_TYPE.add(Long.class);
        BASIC_BOXING_TYPE.add(Byte.class);
        BASIC_BOXING_TYPE.add(Short.class);
        BASIC_BOXING_TYPE.add(Character.class);
        BASIC_BOXING_TYPE.add(String.class);
    }


    /**
     * 检查指定的类型是否是八种基本数据类型其中的一个(不包括void)
     */
    public static boolean isBasicDataType(Class<?> clazz) {
        if (clazz == null || clazz == Void.TYPE) {
            return false;
        }
        return clazz.isPrimitive() || BASIC_BOXING_TYPE.contains(clazz);
    }


    /**
     * 获得用户定义的一个指定的方法，
     *
     * @param clazz      指定的类
     * @param name       方法名称
     * @param paramTypes 方法参数，如果该参数为空，将获取任意方法名称为指定name的方法
     */
    public static Method findMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
        Class<?> searchType = clazz;
        while (searchType != null) {
            Method[] methods = (searchType.isInterface() ? searchType.getMethods() :
                    getDeclaredMethods(searchType));
            for (Method method : methods) {
                if (name.equals(method.getName()) && (paramTypes == null || hasSameParams(method, paramTypes))) {
                    return method;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    // 检查方法参数是否相同
    private static boolean hasSameParams(Method method, Class<?>[] paramTypes) {
        return (paramTypes.length == method.getParameterCount() &&
                Arrays.equals(paramTypes, method.getParameterTypes()));
    }

    // 获取Java8的接口的默认实现方法
    private static List<Method> findConcreteMethodsOnInterfaces(Class<?> clazz) {
        List<Method> result = null;
        for (Class<?> ifc : clazz.getInterfaces()) {
            for (Method ifcMethod : ifc.getMethods()) {
                if (!Modifier.isAbstract(ifcMethod.getModifiers())) {
                    if (result == null) {
                        result = new ArrayList<>();
                    }
                    result.add(ifcMethod);
                }
            }
        }
        return result;
    }

    // 获取类对象上的所有定义的方法，包括Java8的接口的默认实现
    private static Method[] getDeclaredMethods(Class<?> clazz) {
        // 获得不包含继承的方法
        Method[] declaredMethods = clazz.getDeclaredMethods();
        // Java8 接口默认实现
        List<Method> defaultMethods = findConcreteMethodsOnInterfaces(clazz);
        if (defaultMethods != null) {
            Method[] result = new Method[declaredMethods.length + defaultMethods.size()];
            System.arraycopy(declaredMethods, 0, result, 0, declaredMethods.length);
            int index = declaredMethods.length;
            for (Method defaultMethod : defaultMethods) result[index++] = defaultMethod;
            return result;
        }
        return declaredMethods;
    }
}
