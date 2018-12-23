#pragma once

#include <functional>
#include <iterator>
#include <type_traits>

/// Replaces the operator* for the given iterator by some transformation.
/// TODO: Make this work with random access iterators (so that functions like upper_bound()
/// run quickly)
template <typename Iterator, typename T>
class TransformIterator
{
public:
    // iterator_traits requirements
    using difference_type = typename std::iterator_traits<Iterator>::difference_type;
    using value_type = std::remove_cv_t<std::remove_reference_t<T>>;
    using pointer = std::add_pointer_t<value_type>;
    using reference = std::add_lvalue_reference_t<value_type>;
    using iterator_category = std::forward_iterator_tag; // TODO: Should work with random access iterators.

    using TransformFunction = std::function<T(typename std::iterator_traits<Iterator>::reference)>;
    using ThisType = TransformIterator<Iterator, T>;

    TransformIterator(Iterator itr, TransformFunction transform)
        : mIterator(itr)
        , mTransform(transform)
    {
    }

    /* Iterator requirements (except Swappable and iterator traits) */

    // Destructible.
    ~TransformIterator() = default;

    // CopyConstructible.
    TransformIterator(const ThisType &other) = default;

    // CopyAssignable.
    TransformIterator &operator=(const ThisType &other) = default;

    // Dereferencing operator.
    T operator*() { return mTransform(*mIterator); }

    // Prefix increment.
    ThisType &operator++()
    {
        ++mIterator;
        return *this;
    }

    /* Random other things */

    bool operator==(const ThisType &other) { return mIterator == other.mIterator; }
    bool operator!=(const ThisType &other) { return mIterator != other.mIterator; }

    Iterator underlyingIterator() const { return mIterator; }

private:
    Iterator mIterator;
    TransformFunction mTransform;
};
