package stores

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
	"lukechampine.com/frand"
)

type (
	dbMultipartUpload struct {
		Model

		Key        []byte
		UploadID   string `gorm:"uniqueIndex;NOT NULL;size:64"`
		ObjectID   string `gorm:"index;NOT NULL"`
		DBBucket   dbBucket
		DBBucketID uint              `gorm:"index;NOT NULL"`
		Parts      []dbMultipartPart `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete parts too
	}

	dbMultipartPart struct {
		Model
		Etag                string `gorm:"index"`
		PartNumber          int    `gorm:"index"`
		Size                uint64
		DBMultipartUploadID uint      `gorm:"index;NOT NULL"`
		Slabs               []dbSlice `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slices too
	}
)

func (dbMultipartUpload) TableName() string {
	return "multipart_uploads"
}

func (dbMultipartPart) TableName() string {
	return "multipart_parts"
}

func (s *SQLStore) CreateMultipartUpload(ctx context.Context, bucket, path string, ec object.EncryptionKey) (api.MultipartCreateResponse, error) {
	// Marshal key
	key, err := ec.MarshalText()
	if err != nil {
		return api.MultipartCreateResponse{}, err
	}
	var uploadID string
	err = s.retryTransaction(func(tx *gorm.DB) error {
		// Get bucket id.
		var bucketID uint
		err := tx.Table("(SELECT id from buckets WHERE buckets.name = ?) bucket_id", bucket).
			Take(&bucketID).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("bucket %v not found: %w", bucket, api.ErrBucketNotFound)
		} else if err != nil {
			return fmt.Errorf("failed to fetch bucket id: %w", err)
		}
		// Create multipart upload
		uploadIDEntropy := frand.Entropy256()
		uploadID = hex.EncodeToString(uploadIDEntropy[:])
		if err := s.db.Create(&dbMultipartUpload{
			DBBucketID: bucketID,
			Key:        key,
			UploadID:   uploadID,
			ObjectID:   path,
		}).Error; err != nil {
			return fmt.Errorf("failed to create multipart upload: %w", err)
		}
		return nil
	})
	return api.MultipartCreateResponse{
		UploadID: uploadID,
	}, err
}

func (s *SQLStore) AddMultipartPart(ctx context.Context, bucket, path, contractSet, uploadID string, partNumber int, slices []object.SlabSlice, partialSlabs []object.PartialSlab, etag string, usedContracts map[types.PublicKey]types.FileContractID) (err error) {
	return s.retryTransaction(func(tx *gorm.DB) error {
		// Fetch contract set.
		var cs dbContractSet
		if err := tx.Take(&cs, "name = ?", contractSet).Error; err != nil {
			return fmt.Errorf("contract set %v not found: %w", contractSet, err)
		}
		// Fetch the used contracts.
		contracts, err := fetchUsedContracts(tx, usedContracts)
		if err != nil {
			return fmt.Errorf("failed to fetch used contracts: %w", err)
		}
		// Find multipart upload.
		var mu dbMultipartUpload
		err = tx.Where("upload_id", uploadID).
			Take(&mu).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch multipart upload: %w", err)
		}
		// Delete a potentially existing part.
		err = tx.Model(&dbMultipartPart{}).
			Where("db_multipart_upload_id = ? AND part_number = ?", mu.ID, partNumber).
			Delete(&dbMultipartPart{}).
			Error
		if err != nil {
			return fmt.Errorf("failed to delete existing part: %w", err)
		}
		var size uint64
		for _, slice := range slices {
			size += uint64(slice.Length)
		}
		for _, ps := range partialSlabs {
			size += uint64(ps.Length)
		}
		// Create a new part.
		part := dbMultipartPart{
			Etag:                etag,
			PartNumber:          partNumber,
			DBMultipartUploadID: mu.ID,
			Size:                size,
		}
		err = tx.Create(&part).Error
		if err != nil {
			return fmt.Errorf("failed to create part: %w", err)
		}
		// Create the slices.
		err = s.createSlices(tx, nil, &part.ID, cs.ID, contracts, slices, partialSlabs)
		if err != nil {
			return fmt.Errorf("failed to create slices: %w", err)
		}
		return nil
	})
}

// TODO: f/u with support for 'prefix', 'keyMarker' and 'uploadIDMarker'
func (s *SQLStore) MultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) (resp api.MultipartListUploadsResponse, err error) {
	err = s.retryTransaction(func(tx *gorm.DB) error {
		var dbUploads []dbMultipartUpload
		err := tx.Limit(int(maxUploads)).
			Find(&dbUploads).
			Error
		if err != nil {
			return err
		}
		for _, upload := range dbUploads {
			resp.Uploads = append(resp.Uploads, api.MultipartListUploadItem{
				Path:      upload.ObjectID,
				UploadID:  upload.UploadID,
				CreatedAt: upload.CreatedAt.UTC(),
			})
		}
		return nil
	})
	return
}

func (s *SQLStore) MultipartUploadParts(ctx context.Context, bucket, object string, uploadID string, marker int, limit int64) (resp api.MultipartListPartsResponse, _ error) {
	limitUsed := limit > 0
	if !limitUsed {
		limit = math.MaxInt64
	} else {
		limit++
	}

	err := s.retryTransaction(func(tx *gorm.DB) error {
		var dbParts []dbMultipartPart
		err := tx.
			Model(&dbMultipartPart{}).
			Joins("INNER JOIN multipart_uploads mus ON mus.id = multipart_parts.db_multipart_upload_id").
			Joins("INNER JOIN buckets b ON b.name = ? AND b.id = mus.db_bucket_id", bucket).
			Where("mus.object_id = ? AND mus.upload_id = ? AND part_number > ?", object, uploadID, marker).
			Order("part_number ASC").
			Limit(int(limit)).
			Find(&dbParts).
			Error
		if err != nil {
			return err
		}
		// Check if there are more parts beyond 'limit'.
		if limitUsed && len(dbParts) == int(limit) {
			resp.IsTruncated = true
			resp.NextMarker = dbParts[len(dbParts)-1].PartNumber
			dbParts = dbParts[:len(dbParts)-1]
		}
		for _, part := range dbParts {
			resp.Parts = append(resp.Parts, api.MultipartListPartItem{
				PartNumber:   part.PartNumber,
				LastModified: part.CreatedAt.UTC(),
				ETag:         part.Etag,
				Size:         int64(part.Size),
			})
		}
		return nil
	})
	return resp, err
}

func (s *SQLStore) AbortMultipartUpload(ctx context.Context, bucket, path string, uploadID string) error {
	return s.retryTransaction(func(tx *gorm.DB) error {
		// Find multipart upload.
		var mu dbMultipartUpload
		err := tx.Where("upload_id = ?", uploadID).
			Preload("Parts").
			Joins("DBBucket").
			Take(&mu).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch multipart upload: %w", err)
		}
		if mu.ObjectID != path {
			// Check object id.
			return fmt.Errorf("object id mismatch: %v != %v: %w", mu.ObjectID, path, api.ErrObjectNotFound)
		} else if mu.DBBucket.Name != bucket {
			// Check bucket name.
			return fmt.Errorf("bucket name mismatch: %v != %v: %w", mu.DBBucket.Name, bucket, api.ErrBucketNotFound)
		}
		err = tx.Delete(&mu).Error
		if err != nil {
			return fmt.Errorf("failed to delete multipart upload: %w", err)
		}
		return pruneSlabs(tx)
	})
}

func (s *SQLStore) CompleteMultipartUpload(ctx context.Context, bucket, path string, uploadID string, parts []api.MultipartCompletedPart) (_ api.MultipartCompleteResponse, err error) {
	// Sanity check input parts.
	if !sort.SliceIsSorted(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	}) {
		return api.MultipartCompleteResponse{}, fmt.Errorf("provided parts are not sorted")
	}
	for i := 0; i < len(parts)-1; i++ {
		if parts[i].PartNumber == parts[i+1].PartNumber {
			return api.MultipartCompleteResponse{}, fmt.Errorf("duplicate part number %v", parts[i].PartNumber)
		}
	}
	var etag string
	err = s.retryTransaction(func(tx *gorm.DB) error {
		// Find multipart upload.
		var mu dbMultipartUpload
		err = tx.Where("upload_id = ?", uploadID).
			Preload("Parts").
			Joins("DBBucket").
			Take(&mu).
			Error
		if err != nil {
			return fmt.Errorf("failed to fetch multipart upload: %w", err)
		}
		// Check object id.
		if mu.ObjectID != path {
			return fmt.Errorf("object id mismatch: %v != %v: %w", mu.ObjectID, path, api.ErrObjectNotFound)
		}

		// Check bucket name.
		if mu.DBBucket.Name != bucket {
			return fmt.Errorf("bucket name mismatch: %v != %v: %w", mu.DBBucket.Name, bucket, api.ErrBucketNotFound)
		}
		// Sort the parts.
		sort.Slice(mu.Parts, func(i, j int) bool {
			return mu.Parts[i].PartNumber < mu.Parts[j].PartNumber
		})
		// Find relevant parts.
		var dbParts []dbMultipartPart
		var size uint64
		j := 0
		for _, part := range parts {
			for {
				if j >= len(mu.Parts) {
					// ran out of parts in the database
					return api.ErrPartNotFound
				} else if mu.Parts[j].PartNumber > part.PartNumber {
					// missing part
					return api.ErrPartNotFound
				} else if mu.Parts[j].PartNumber == part.PartNumber && mu.Parts[j].Etag == part.ETag {
					// found a match
					dbParts = append(dbParts, mu.Parts[j])
					size += mu.Parts[j].Size
					j++
					break
				} else {
					// try next
					j++
				}
			}
		}

		// Fetch all the slices in the right order.
		var slices []dbSlice
		h := types.NewHasher()
		for _, part := range dbParts {
			var partSlices []dbSlice
			err = tx.Model(&dbSlice{}).
				Joins("INNER JOIN multipart_parts mp ON mp.id = slices.db_multipart_part_id AND mp.id = ?", part.ID).
				Joins("INNER JOIN multipart_uploads mus ON mus.id = mp.db_multipart_upload_id").
				Find(&partSlices).
				Error
			if err != nil {
				return fmt.Errorf("failed to fetch slices: %w", err)
			}
			slices = append(slices, partSlices...)
			if _, err = h.E.Write([]byte(part.Etag)); err != nil {
				return fmt.Errorf("failed to hash etag: %w", err)
			}
		}

		// Compute etag.
		sum := h.Sum()
		etag = hex.EncodeToString(sum[:])

		// Sort their primary keys to make sure retrieving them later will
		// respect the part order.
		sort.Sort(sortedSlices(slices))

		// Create the object.
		obj := dbObject{
			DBBucketID: mu.DBBucketID,
			ObjectID:   path,
			Key:        mu.Key,
			Size:       int64(size),
		}
		if err := tx.Create(&obj).Error; err != nil {
			return fmt.Errorf("failed to create object: %w", err)
		}

		// Assign the right object id and unassign the multipart upload.
		for i := range slices {
			slices[i].DBObjectID = &obj.ID
			slices[i].DBMultipartPartID = nil
		}

		// Save updated slices.
		if err := tx.Save(slices).Error; err != nil {
			return fmt.Errorf("failed to save slices: %w", err)
		}

		// Delete the multipart upload.
		if err := tx.Delete(&mu).Error; err != nil {
			return fmt.Errorf("failed to delete multipart upload: %w", err)
		}
		return nil
	})
	if err != nil {
		return api.MultipartCompleteResponse{}, err
	}
	return api.MultipartCompleteResponse{
		ETag: etag,
	}, nil
}

type sortedSlices []dbSlice

func (s sortedSlices) Len() int {
	return len(s)
}

func (s sortedSlices) Less(i, j int) bool {
	return s[i].ID < s[j].ID
}

func (s sortedSlices) Swap(i, j int) {
	s[i].ID, s[j].ID = s[j].ID, s[i].ID
}
