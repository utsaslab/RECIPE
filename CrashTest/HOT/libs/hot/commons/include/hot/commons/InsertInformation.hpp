#ifndef __HOT__COMMONS__INSERT_INFORMATION___
#define __HOT__COMMONS__INSERT_INFORMATION___

#include <cassert>

#include <hot/commons/DiscriminativeBit.hpp>

namespace hot { namespace commons {

/**
 * The insert information contains all information, which is necessary to actually insert a new key.
 * It consists of:
 *  + the discriminative bit, which discriminates the entries in the affected subtree from the new entry
 * 	+ a partial key which contains all discriminative bits,
 * 	  which are shared by the entries in the affected subtree and the new key to insert
 * 	+ the positions of the entries in the affected subtree
 *
 */
struct InsertInformation {
	/**
	 * a partial key containing all discriminative bits, which are shared by the entries in the affected subtree
	 * and the new key.
	 */
	uint32_t const mSubtreePrefixPartialKey;
	uint32_t const mFirstIndexInAffectedSubtree;
	uint32_t const mNumberEntriesInAffectedSubtree;
	DiscriminativeBit const mKeyInformation;

	InsertInformation(uint32_t const subtreePrefixPartialKey, uint32_t const firstIndexInAffectedSubtree,
					  uint32_t const numberEntriesInAffectedSubtree, DiscriminativeBit const & keyInformation
	)
		: mSubtreePrefixPartialKey(subtreePrefixPartialKey), mFirstIndexInAffectedSubtree(firstIndexInAffectedSubtree),
		  mNumberEntriesInAffectedSubtree(numberEntriesInAffectedSubtree),
		  mKeyInformation(keyInformation)
	{
		assert(numberEntriesInAffectedSubtree > 0);
	}

	unsigned int getFirstIndexInAffectedSubtree() const {
		return mFirstIndexInAffectedSubtree;
	}

	unsigned int getNumberEntriesInAffectedSubtree() const {
		return mNumberEntriesInAffectedSubtree;
	}

};

} }

#endif