#ifndef __IDX__BENCHMARK__CONTINUOS_BUFFER_HPP__
#define __IDX__BENCHMARK__CONTINUOS_BUFFER_HPP__

namespace idx { namespace benchmark {

struct ContinuousBuffer {
	void* mRemainingBuffer;
	size_t mRemainingBufferSize;

	inline ContinuousBuffer(void* remainingBuffer, size_t remainingBufferSize)
		: mRemainingBuffer(remainingBuffer), mRemainingBufferSize(remainingBufferSize)
	{
	}

	template<typename ContentTypeToAlignTo> inline ContinuousBuffer align() const {
		ContinuousBuffer targetBuffer(*this);
		void* targetMemory = std::align(std::alignment_of<ContentTypeToAlignTo>(), sizeof(ContentTypeToAlignTo), targetBuffer.mRemainingBuffer, targetBuffer.mRemainingBufferSize);

		if(targetMemory == nullptr) { //not enough memory provided
			throw std::bad_alloc();
		}

		return targetBuffer;
	}

	inline ContinuousBuffer advance(size_t bytesToAdvance) const {
		if(bytesToAdvance > mRemainingBufferSize) {
			throw std::bad_alloc();
		}
		return {
			reinterpret_cast<char*>(mRemainingBuffer) + bytesToAdvance,
			mRemainingBufferSize - bytesToAdvance
		};
	}

	template<typename ContentType> inline std::pair<ContentType*, ContinuousBuffer> allocate() const {
		const ContinuousBuffer & contentBuffer = align<ContentType>();
		const ContinuousBuffer & nextBuffer = contentBuffer.advance(sizeof(ContentType));
		return std::make_pair(new (contentBuffer.mRemainingBuffer) ContentType(), nextBuffer);
	};
};

} }

#endif