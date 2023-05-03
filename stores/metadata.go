package stores

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"gorm.io/gorm"
)

const (
	// slabRetrievalBatchSize is the number of slabs we fetch from the
	// database per batch
	// NOTE: This value can't be too big or otherwise UnhealthySlabs will fail
	// due to "too many SQL variables".
	slabRetrievalBatchSize = 100
)

var (
	// ErrSlabNotFound is returned if get is unable to retrieve a slab from the
	// database.
	ErrSlabNotFound = errors.New("slab not found in database")

	// ErrContractNotFound is returned when a contract can't be retrieved from
	// the database.
	ErrContractNotFound = errors.New("couldn't find contract")
)

type (
	dbArchivedContract struct {
		Model

		ContractCommon
		RenewedTo fileContractID `gorm:"index;size:32"`

		Host   publicKey `gorm:"index;NOT NULL;size:32"`
		Reason string
	}

	dbContract struct {
		Model

		ContractCommon

		HostID uint `gorm:"index"`
		Host   dbHost
	}

	ContractCommon struct {
		FCID        fileContractID `gorm:"unique;index;NOT NULL;column:fcid;size:32"`
		RenewedFrom fileContractID `gorm:"index;size:32"`

		TotalCost      currency
		ProofHeight    uint64 `gorm:"index;default:0"`
		RevisionHeight uint64 `gorm:"index;default:0"`
		RevisionNumber string `gorm:"NOT NULL;default:'0'"` // string since db can't store math.MaxUint64
		StartHeight    uint64 `gorm:"index;NOT NULL"`
		WindowStart    uint64 `gorm:"index;NOT NULL;default:0"`
		WindowEnd      uint64 `gorm:"index;NOT NULL;default:0"`

		// spending fields
		UploadSpending      currency
		DownloadSpending    currency
		FundAccountSpending currency
	}

	dbContractSet struct {
		Model

		Name      string       `gorm:"unique;index"`
		Contracts []dbContract `gorm:"many2many:contract_set_contracts;constraint:OnDelete:CASCADE"`
	}

	dbObject struct {
		Model

		Key      []byte
		ObjectID string    `gorm:"index;unique"`
		Slabs    []dbSlice `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slices too
		Size     int64
	}

	dbSlice struct {
		Model
		DBObjectID uint `gorm:"index"`

		// Slice related fields.
		Slab   dbSlab `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete slabs too
		Offset uint32
		Length uint32
	}

	dbSlab struct {
		Model
		DBSliceID uint `gorm:"index"`

		Key         []byte `gorm:"unique;NOT NULL;size:68"` // json string
		MinShards   uint8
		TotalShards uint8
		Shards      []dbShard `gorm:"constraint:OnDelete:CASCADE"` // CASCADE to delete shards too
	}

	dbSector struct {
		Model

		LatestHost publicKey `gorm:"NOT NULL"`
		Root       []byte    `gorm:"index;unique;NOT NULL;size:32"`

		Contracts []dbContract `gorm:"many2many:contract_sectors;constraint:OnDelete:CASCADE"`
		Hosts     []dbHost     `gorm:"many2many:host_sectors;constraint:OnDelete:CASCADE"`
	}

	// dbContractSector is a join table between dbContract and dbSector.
	dbContractSector struct {
		DBContractID uint `gorm:"primaryKey"`
		DBSectorID   uint `gorm:"primaryKey"`
	}

	// dbShard is a join table between dbSlab and dbSector.
	dbShard struct {
		ID         uint `gorm:"primaryKey"`
		DBSlabID   uint `gorm:"index"`
		DBSector   dbSector
		DBSectorID uint `gorm:"index"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbArchivedContract) TableName() string { return "archived_contracts" }

// TableName implements the gorm.Tabler interface.
func (dbContract) TableName() string { return "contracts" }

// TableName implements the gorm.Tabler interface.
func (dbContractSector) TableName() string { return "contract_sectors" }

// TableName implements the gorm.Tabler interface.
func (dbContractSet) TableName() string { return "contract_sets" }

// TableName implements the gorm.Tabler interface.
func (dbObject) TableName() string { return "objects" }

// TableName implements the gorm.Tabler interface.
func (dbSector) TableName() string { return "sectors" }

// TableName implements the gorm.Tabler interface.
func (dbShard) TableName() string { return "shards" }

// TableName implements the gorm.Tabler interface.
func (dbSlab) TableName() string { return "slabs" }

// TableName implements the gorm.Tabler interface.
func (dbSlice) TableName() string { return "slices" }

// convert converts a dbContract to an ArchivedContract.
func (c dbArchivedContract) convert() api.ArchivedContract {
	var revisionNumber uint64
	_, _ = fmt.Sscan(c.RevisionNumber, &revisionNumber)
	return api.ArchivedContract{
		ID:        types.FileContractID(c.FCID),
		HostKey:   types.PublicKey(c.Host),
		RenewedTo: types.FileContractID(c.RenewedTo),

		ProofHeight:    c.ProofHeight,
		RevisionHeight: c.RevisionHeight,
		RevisionNumber: revisionNumber,
		StartHeight:    c.StartHeight,
		WindowStart:    c.WindowStart,
		WindowEnd:      c.WindowEnd,

		Spending: api.ContractSpending{
			Uploads:     types.Currency(c.UploadSpending),
			Downloads:   types.Currency(c.DownloadSpending),
			FundAccount: types.Currency(c.FundAccountSpending),
		},
	}
}

// convert converts a dbContract to a ContractMetadata.
func (c dbContract) convert() api.ContractMetadata {
	var revisionNumber uint64
	_, _ = fmt.Sscan(c.RevisionNumber, &revisionNumber)
	return api.ContractMetadata{
		ID:         types.FileContractID(c.FCID),
		HostIP:     c.Host.NetAddress,
		HostKey:    types.PublicKey(c.Host.PublicKey),
		SiamuxAddr: c.Host.Settings.convert().SiamuxAddr(),

		RenewedFrom: types.FileContractID(c.RenewedFrom),
		TotalCost:   types.Currency(c.TotalCost),
		Spending: api.ContractSpending{
			Uploads:     types.Currency(c.UploadSpending),
			Downloads:   types.Currency(c.DownloadSpending),
			FundAccount: types.Currency(c.FundAccountSpending),
		},
		ProofHeight:    c.ProofHeight,
		RevisionHeight: c.RevisionHeight,
		RevisionNumber: revisionNumber,
		StartHeight:    c.StartHeight,
		WindowStart:    c.WindowStart,
		WindowEnd:      c.WindowEnd,
	}
}

// convert turns a dbObject into a object.Slab.
func (s dbSlab) convert() (slab object.Slab, err error) {
	// unmarshal key
	err = slab.Key.UnmarshalText(s.Key)
	if err != nil {
		return
	}

	// set shards
	slab.MinShards = s.MinShards
	slab.Shards = make([]object.Sector, len(s.Shards))

	// hydrate shards if possible
	for i, shard := range s.Shards {
		if shard.DBSector.ID == 0 {
			continue // sector wasn't preloaded
		}

		slab.Shards[i].Host = types.PublicKey(shard.DBSector.LatestHost)
		slab.Shards[i].Root = *(*types.Hash256)(shard.DBSector.Root)
	}

	return
}

func (o dbObject) metadata() api.ObjectMetadata {
	return api.ObjectMetadata{
		Name: o.ObjectID,
		Size: o.Size,
	}
}

// convert turns a dbObject into a object.Object.
func (o dbObject) convert() (object.Object, error) {
	var objKey object.EncryptionKey
	if err := objKey.UnmarshalText(o.Key); err != nil {
		return object.Object{}, err
	}
	obj := object.Object{
		Key:   objKey,
		Slabs: make([]object.SlabSlice, len(o.Slabs)),
	}
	for i, sl := range o.Slabs {
		slab, err := sl.Slab.convert()
		if err != nil {
			return object.Object{}, err
		}
		obj.Slabs[i] = object.SlabSlice{
			Slab:   slab,
			Offset: sl.Offset,
			Length: sl.Length,
		}
	}
	return obj, nil
}

// ObjectsStats returns some info related to the objects stored in the store. To
// reduce locking and make sure all results are consistent, everything is done
// within a single transaction.
func (s *SQLStore) ObjectsStats(ctx context.Context) (api.ObjectsStats, error) {
	var resp api.ObjectsStats
	return resp, s.db.Transaction(func(tx *gorm.DB) error {
		// Number of objects.
		err := tx.
			Model(&dbObject{}).
			Select("COUNT(*)").
			Scan(&resp.NumObjects).
			Error
		if err != nil {
			return err
		}
		// Size of objects.
		err = tx.
			Model(&dbSlice{}).
			Select("SUM(length)").
			Scan(&resp.TotalObjectsSize).
			Error
		if err != nil {
			return err
		}
		// Size of sectors
		var sectorSizes struct {
			SectorsSize  uint64
			UploadedSize uint64
		}
		err = tx.
			Model(&dbContractSector{}).
			Select("COUNT(DISTINCT db_sector_id) * ? as sectors_size, COUNT(*) * ? as uploaded_size", rhpv2.SectorSize, rhpv2.SectorSize).
			Scan(&sectorSizes).
			Error
		if err != nil {
			return err
		}
		resp.TotalSectorsSize = sectorSizes.SectorsSize
		resp.TotalUploadedSize = sectorSizes.UploadedSize
		return nil
	})
}

func (s *SQLStore) AddContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64) (_ api.ContractMetadata, err error) {
	var added dbContract
	if err = s.retryTransaction(func(tx *gorm.DB) error {
		added, err = addContract(tx, c, totalCost, startHeight, types.FileContractID{})
		return err
	}); err != nil {
		return
	}

	s.knownContracts[types.FileContractID(added.FCID)] = struct{}{}
	return added.convert(), nil
}

func (s *SQLStore) ActiveContracts(ctx context.Context) ([]api.ContractMetadata, error) {
	var dbContracts []dbContract
	err := s.db.
		Model(&dbContract{}).
		Preload("Host").
		Find(&dbContracts).
		Error
	if err != nil {
		return nil, err
	}

	contracts := make([]api.ContractMetadata, len(dbContracts))
	for i, c := range dbContracts {
		contracts[i] = c.convert()
	}
	return contracts, nil
}

// AddRenewedContract adds a new contract which was created as the result of a renewal to the store.
// The old contract specified as 'renewedFrom' will be deleted from the active
// contracts and moved to the archive. Both new and old contract will be linked
// to each other through the RenewedFrom and RenewedTo fields respectively.
func (s *SQLStore) AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (api.ContractMetadata, error) {
	var renewed dbContract

	if err := s.retryTransaction(func(tx *gorm.DB) error {
		// Fetch contract we renew from.
		oldContract, err := contract(tx, fileContractID(renewedFrom))
		if err != nil {
			return err
		}

		// Create copy in archive.
		err = tx.Create(&dbArchivedContract{
			Host:      publicKey(oldContract.Host.PublicKey),
			Reason:    api.ContractArchivalReasonRenewed,
			RenewedTo: fileContractID(c.ID()),

			ContractCommon: oldContract.ContractCommon,
		}).Error
		if err != nil {
			return err
		}

		// Add the new contract.
		renewed, err = addContract(tx, c, totalCost, startHeight, renewedFrom)
		if err != nil {
			return err
		}
		s.knownContracts[c.ID()] = struct{}{}

		// Update the old contract in the contract set to the new one.
		err = tx.Table("contract_set_contracts").
			Where("db_contract_id = ?", oldContract.ID).
			Update("db_contract_id", renewed.ID).Error
		if err != nil {
			return err
		}

		// Update the contract_sectors table from the old contract to the new
		// one.
		err = tx.Table("contract_sectors").
			Where("db_contract_id = ?", oldContract.ID).
			Update("db_contract_id", renewed.ID).Error
		if err != nil {
			return err
		}

		// Finally delete the old contract.
		res := tx.Delete(&oldContract)
		if err := res.Error; err != nil {
			return err
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("expected to delete 1 row, deleted %d", res.RowsAffected)
		}

		return nil
	}); err != nil {
		return api.ContractMetadata{}, err
	}

	return renewed.convert(), nil
}

func (s *SQLStore) AncestorContracts(ctx context.Context, id types.FileContractID, startHeight uint64) ([]api.ArchivedContract, error) {
	var ancestors []dbArchivedContract
	err := s.db.Raw("WITH RECURSIVE ancestors AS (SELECT * FROM archived_contracts WHERE renewed_to = ? UNION ALL SELECT archived_contracts.* FROM ancestors, archived_contracts WHERE archived_contracts.renewed_to = ancestors.fcid) SELECT * FROM ancestors WHERE start_height >= ?", fileContractID(id), startHeight).
		Scan(&ancestors).
		Error
	if err != nil {
		return nil, err
	}
	contracts := make([]api.ArchivedContract, len(ancestors))
	for i, ancestor := range ancestors {
		contracts[i] = ancestor.convert()
	}
	return contracts, nil
}

func (s *SQLStore) ArchiveContract(ctx context.Context, id types.FileContractID, reason string) error {
	return s.ArchiveContracts(ctx, map[types.FileContractID]string{id: reason})
}

func (s *SQLStore) ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error {
	// fetch ids
	var ids []types.FileContractID
	for id := range toArchive {
		ids = append(ids, id)
	}

	// fetch contracts
	cs, err := contracts(s.db, ids)
	if err != nil {
		return err
	}

	// archive them
	if err := s.retryTransaction(func(tx *gorm.DB) error {
		return archiveContracts(tx, cs, toArchive)
	}); err != nil {
		return err
	}

	return nil
}

func (s *SQLStore) ArchiveAllContracts(ctx context.Context, reason string) error {
	// fetch contract ids
	var fcids []fileContractID
	if err := s.db.
		Model(&dbContract{}).
		Pluck("fcid", &fcids).
		Error; err != nil {
		return err
	}

	// create map
	toArchive := make(map[types.FileContractID]string)
	for _, fcid := range fcids {
		toArchive[types.FileContractID(fcid)] = reason
	}

	return s.ArchiveContracts(ctx, toArchive)
}

func (s *SQLStore) Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error) {
	contract, err := s.contract(ctx, fileContractID(id))
	if err != nil {
		return api.ContractMetadata{}, err
	}
	return contract.convert(), nil
}

func (s *SQLStore) Contracts(ctx context.Context, set string) ([]api.ContractMetadata, error) {
	dbContracts, err := s.contracts(ctx, set)
	if err != nil {
		return nil, err
	}
	contracts := make([]api.ContractMetadata, len(dbContracts))
	for i, c := range dbContracts {
		contracts[i] = c.convert()
	}
	return contracts, nil
}

func (s *SQLStore) ContractSets(ctx context.Context) ([]string, error) {
	var sets []string
	err := s.db.Raw("SELECT name FROM contract_sets").
		Scan(&sets).
		Error
	return sets, err
}

func (s *SQLStore) SetContractSet(ctx context.Context, name string, contractIds []types.FileContractID) error {
	fcids := make([]fileContractID, len(contractIds))
	for i, fcid := range contractIds {
		fcids[i] = fileContractID(fcid)
	}

	// fetch contracts
	var dbContracts []dbContract
	err := s.db.
		Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Find(&dbContracts).
		Error
	if err != nil {
		return err
	}

	// create contract set
	var contractset dbContractSet
	err = s.db.
		Where(dbContractSet{Name: name}).
		FirstOrCreate(&contractset).
		Error
	if err != nil {
		return err
	}

	// update contracts
	return s.db.Model(&contractset).Association("Contracts").Replace(&dbContracts)
}

func (s *SQLStore) RemoveContractSet(ctx context.Context, name string) error {
	return s.db.
		Where(dbContractSet{Name: name}).
		Delete(&dbContractSet{}).
		Error
}

func (s *SQLStore) SearchObjects(ctx context.Context, substring string, offset, limit int) ([]api.ObjectMetadata, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}

	var objects []dbObject
	err := s.db.Model(&dbObject{}).
		Where("object_id LIKE ?", "%"+substring+"%").
		Offset(offset).
		Limit(limit).
		Find(&objects).Error
	if err != nil {
		return nil, err
	}
	metadata := make([]api.ObjectMetadata, len(objects))
	for i, entry := range objects {
		metadata[i] = entry.metadata()
	}
	return metadata, nil
}

func (s *SQLStore) ObjectEntries(ctx context.Context, path, prefix string, offset, limit int) ([]api.ObjectMetadata, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}
	if !strings.HasSuffix(path, "/") {
		panic("path must end in /")
	}

	concat := func(a, b string) string {
		if isSQLite(s.db) {
			return fmt.Sprintf("%s || %s", a, b)
		}
		return fmt.Sprintf("CONCAT(%s, %s)", a, b)
	}

	// base query
	query := s.db.Raw(fmt.Sprintf(`SELECT SUM(size) AS size, CASE slashindex WHEN 0 THEN %s ELSE %s END AS name
	FROM (
		SELECT size, trimmed, INSTR(trimmed, ?) AS slashindex
		FROM (
			SELECT size, SUBSTR(object_id, ?) AS trimmed
			FROM objects
			WHERE object_id LIKE ?
		) AS i
	) AS m
	GROUP BY name
	LIMIT ? OFFSET ?`, concat("?", "trimmed"), concat("?", "substr(trimmed, 1, slashindex)")), path, path, "/", utf8.RuneCountInString(path)+1, path+"%", limit, offset)

	// apply prefix
	if prefix != "" {
		query = s.db.Raw(fmt.Sprintf("SELECT * FROM (?) AS i WHERE name LIKE %s", concat("?", "?")), query, path, prefix+"%")
	}

	var metadata []api.ObjectMetadata
	err := query.Scan(&metadata).Error
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func (s *SQLStore) Object(ctx context.Context, key string) (object.Object, error) {
	obj, err := s.object(ctx, key)
	if err != nil {
		return object.Object{}, err
	}
	return obj.convert()
}

func (s *SQLStore) RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error {
	squashedRecords := make(map[types.FileContractID]api.ContractSpending)
	for _, r := range records {
		squashedRecords[r.ContractID] = squashedRecords[r.ContractID].Add(r.ContractSpending)
	}
	for fcid, newSpending := range squashedRecords {
		err := s.retryTransaction(func(tx *gorm.DB) error {
			var contract dbContract
			err := tx.Model(&dbContract{}).
				Where("fcid = ?", fileContractID(fcid)).
				Take(&contract).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil // contract not found, continue with next one
			} else if err != nil {
				return err
			}
			updates := make(map[string]interface{})
			if !newSpending.Uploads.IsZero() {
				updates["upload_spending"] = currency(types.Currency(contract.UploadSpending).Add(newSpending.Uploads))
			}
			if !newSpending.Downloads.IsZero() {
				updates["download_spending"] = currency(types.Currency(contract.DownloadSpending).Add(newSpending.Downloads))
			}
			if !newSpending.FundAccount.IsZero() {
				updates["fund_account_spending"] = currency(types.Currency(contract.FundAccountSpending).Add(newSpending.FundAccount))
			}
			return tx.Model(&contract).Updates(updates).Error
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLStore) UpdateObject(ctx context.Context, key string, o object.Object, usedContracts map[types.PublicKey]types.FileContractID) error {
	// Sanity check input.
	for _, ss := range o.Slabs {
		for _, shard := range ss.Shards {
			// Verify that all hosts have a contract.
			_, exists := usedContracts[shard.Host]
			if !exists {
				return fmt.Errorf("missing contract for host %v", shard.Host)
			}
		}
	}

	// UpdateObject is ACID.
	return s.retryTransaction(func(tx *gorm.DB) error {
		// Try to delete first. We want to get rid of the object and its
		// slabs if it exists.
		err := deleteObject(tx, key)
		if err != nil {
			return err
		}

		// Insert a new object.
		objKey, err := o.Key.MarshalText()
		if err != nil {
			return err
		}
		obj := dbObject{
			ObjectID: key,
			Key:      objKey,
			Size:     o.Size(),
		}
		err = tx.Create(&obj).Error
		if err != nil {
			return err
		}

		for _, ss := range o.Slabs {
			// Create Slice.
			slice := dbSlice{
				DBObjectID: obj.ID,
				Offset:     ss.Offset,
				Length:     ss.Length,
			}
			err = tx.Create(&slice).Error
			if err != nil {
				return err
			}

			// Create Slab.
			slabKey, err := ss.Key.MarshalText()
			if err != nil {
				return err
			}
			slab := &dbSlab{
				DBSliceID:   slice.ID,
				Key:         slabKey,
				MinShards:   ss.MinShards,
				TotalShards: uint8(len(ss.Shards)),
			}
			err = tx.Create(&slab).Error
			if err != nil {
				return err
			}

			for _, shard := range ss.Shards {
				// Translate pubkey to contract.
				fcid := usedContracts[shard.Host]

				// Create sector if it doesn't exist yet.
				var sector dbSector
				err := tx.
					Where(dbSector{Root: shard.Root[:]}).
					Assign(dbSector{LatestHost: publicKey(shard.Host)}).
					FirstOrCreate(&sector).
					Error
				if err != nil {
					return err
				}

				// Add the slab-sector link to the sector to the
				// shards table.
				err = tx.Create(&dbShard{
					DBSlabID:   slab.ID,
					DBSectorID: sector.ID,
				}).Error
				if err != nil {
					return err
				}

				// Look for the contract referenced by the shard.
				contractFound := true
				var contract dbContract
				err = tx.Model(&dbContract{}).
					Where(&dbContract{ContractCommon: ContractCommon{FCID: fileContractID(fcid)}}).
					Take(&contract).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					contractFound = false
				} else if err != nil {
					return err
				}

				// Look for the host referenced by the shard.
				hostFound := true
				var host dbHost
				err = tx.Model(&dbHost{}).
					Where(&dbHost{PublicKey: publicKey(shard.Host)}).
					Take(&host).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					hostFound = false
				} else if err != nil {
					return err
				}

				// Add contract and host to join tables.
				if contractFound {
					err = tx.Model(&sector).Association("Contracts").Append(&contract)
					if err != nil {
						return err
					}
				}
				if hostFound {
					err = tx.Model(&sector).Association("Hosts").Append(&host)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

func (s *SQLStore) RemoveObject(ctx context.Context, key string) error {
	return deleteObject(s.db, key)
}

func (ss *SQLStore) UpdateSlab(ctx context.Context, s object.Slab, usedContracts map[types.PublicKey]types.FileContractID) error {
	// sanity check the shards don't contain an empty root
	for _, s := range s.Shards {
		if s.Root == (types.Hash256{}) {
			return errors.New("shard root can never be the empty root")
		}
	}

	// extract the slab key
	key, err := s.Key.MarshalText()
	if err != nil {
		return err
	}

	// extract host keys
	hostkeys := make([]publicKey, 0, len(usedContracts))
	for key := range usedContracts {
		hostkeys = append(hostkeys, publicKey(key))
	}

	// extract file contract ids
	fcids := make([]fileContractID, 0, len(usedContracts))
	for _, fcid := range usedContracts {
		fcids = append(fcids, fileContractID(fcid))
	}

	// find all hosts
	var dbHosts []dbHost
	if err := ss.db.
		Model(&dbHost{}).
		Where("public_key IN (?)", hostkeys).
		Find(&dbHosts).
		Error; err != nil {
		return err
	}

	// find all contracts
	var dbContracts []dbContract
	if err := ss.db.
		Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Find(&dbContracts).
		Error; err != nil {
		return err
	}

	// make a hosts map
	hosts := make(map[publicKey]*dbHost)
	for i := range dbHosts {
		hosts[dbHosts[i].PublicKey] = &dbHosts[i]
	}

	// make a contracts map
	contracts := make(map[fileContractID]*dbContract)
	for i := range dbContracts {
		contracts[fileContractID(dbContracts[i].FCID)] = &dbContracts[i]
	}

	// find existing slab
	var slab dbSlab
	if err = ss.db.
		Where(&dbSlab{Key: key}).
		Assign(&dbSlab{TotalShards: uint8(len(slab.Shards))}).
		Preload("Shards.DBSector").
		Take(&slab).
		Error; err == gorm.ErrRecordNotFound {
		return fmt.Errorf("slab with key '%s' not found: %w", string(key), err)
	} else if err != nil {
		return err
	}

	// Update slab.
	return ss.retryTransaction(func(tx *gorm.DB) (err error) {
		// build map out of current shards
		shards := make(map[uint]struct{})
		for _, shard := range slab.Shards {
			shards[shard.DBSectorID] = struct{}{}
		}

		// loop updated shards
		for _, shard := range s.Shards {
			// ensure the sector exists
			var sector dbSector
			if err := tx.
				Where(dbSector{Root: shard.Root[:]}).
				Assign(dbSector{LatestHost: publicKey(shard.Host)}).
				FirstOrCreate(&sector).
				Error; err != nil {
				return err
			}

			// ensure the join table has an entry
			_, exists := shards[sector.ID]
			if !exists {
				if err := tx.
					Create(&dbShard{
						DBSlabID:   slab.ID,
						DBSectorID: sector.ID,
					}).Error; err != nil {
					return err
				}
			}

			// ensure the associations are updated
			if contract := contracts[fileContractID(usedContracts[shard.Host])]; contract != nil {
				if err := tx.
					Model(&sector).
					Association("Contracts").
					Append(contract); err != nil {
					return err
				}
			}
			if host := hosts[publicKey(shard.Host)]; host != nil {
				if err := tx.
					Model(&sector).
					Association("Hosts").
					Append(host); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// UnhealthySlabs returns up to 'limit' slabs that do not reach full redundancy
// in the given contract set. These slabs need to be migrated to good contracts
// so they are restored to full health.
func (s *SQLStore) UnhealthySlabs(ctx context.Context, healthCutoff float64, set string, limit int) ([]object.Slab, error) {
	if limit <= -1 {
		limit = math.MaxInt
	}

	var dbBatch []dbSlab
	var slabs []object.Slab

	if err := s.db.
		Select(`slabs.*,
		        CASE
				  WHEN (slabs.min_shards = slabs.total_shards)
				  THEN
				    CASE
					WHEN (COUNT(DISTINCT(c.host_id)) < slabs.min_shards)
					THEN
					  -1
					ELSE
					  1
					END
				  ELSE
				  (CAST(COUNT(DISTINCT(c.host_id)) AS FLOAT) - CAST(slabs.min_shards AS FLOAT)) / Cast(slabs.total_shards - slabs.min_shards AS FLOAT)
				  END AS health`).
		Model(&dbSlab{}).
		Joins("INNER JOIN shards sh ON sh.db_slab_id = slabs.id").
		Joins("INNER JOIN sectors s ON sh.db_sector_id = s.id").
		Joins("LEFT JOIN contract_sectors se USING (db_sector_id)").
		Joins("LEFT JOIN contracts c ON se.db_contract_id = c.id").
		Joins("INNER JOIN contract_set_contracts csc ON csc.db_contract_id = c.id").
		Joins("INNER JOIN contract_sets cs ON cs.id = csc.db_contract_set_id").
		Where("cs.name = ?", set).
		Group("slabs.id").
		Having("health >= 0 AND health <= ?", healthCutoff).
		Order("health ASC").
		Limit(limit).
		Preload("Shards.DBSector").
		FindInBatches(&dbBatch, slabRetrievalBatchSize, func(tx *gorm.DB, batch int) error {
			for _, dbSlab := range dbBatch {
				if slab, err := dbSlab.convert(); err == nil {
					slabs = append(slabs, slab)
				} else {
					panic(err)
				}
			}
			return nil
		}).
		Error; err != nil {
		return nil, err
	}

	return slabs, nil
}

// object retrieves an object from the store.
func (s *SQLStore) object(ctx context.Context, key string) (dbObject, error) {
	var obj dbObject
	tx := s.db.Where(&dbObject{ObjectID: key}).
		Preload("Slabs.Slab.Shards.DBSector.Contracts.Host").
		Take(&obj)
	if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
		return dbObject{}, api.ErrObjectNotFound
	}
	return obj, nil
}

// contract retrieves a contract from the store.
func (s *SQLStore) contract(ctx context.Context, id fileContractID) (dbContract, error) {
	return contract(s.db, id)
}

// contracts retrieves all contracts in the given set.
func (s *SQLStore) contracts(ctx context.Context, set string) ([]dbContract, error) {
	var cs dbContractSet
	err := s.db.
		Where(&dbContractSet{Name: set}).
		Preload("Contracts.Host").
		Take(&cs).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("%w '%s'", api.ErrContractSetNotFound, set)
	} else if err != nil {
		return nil, err
	}

	return cs.Contracts, nil
}

// contract retrieves a contract from the store.
func contract(tx *gorm.DB, id fileContractID) (contract dbContract, err error) {
	err = tx.
		Where(&dbContract{ContractCommon: ContractCommon{FCID: id}}).
		Preload("Host").
		Take(&contract).
		Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = ErrContractNotFound
	}
	return
}

// contracts retrieves all contracts for the given ids from the store.
func contracts(tx *gorm.DB, ids []types.FileContractID) (dbContracts []dbContract, err error) {
	fcids := make([]fileContractID, len(ids))
	for i, fcid := range ids {
		fcids[i] = fileContractID(fcid)
	}

	// fetch contracts
	err = tx.
		Model(&dbContract{}).
		Where("fcid IN (?)", fcids).
		Preload("Host").
		Find(&dbContracts).
		Error
	return
}

// contractsForHost retrieves all contracts for the given host
func contractsForHost(tx *gorm.DB, host dbHost) (contracts []dbContract, err error) {
	err = tx.
		Where(&dbContract{HostID: host.ID}).
		Preload("Host").
		Find(&contracts).
		Error
	return
}

// addContract adds a contract to the store.
func addContract(tx *gorm.DB, c rhpv2.ContractRevision, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID) (dbContract, error) {
	fcid := c.ID()

	// Find host.
	var host dbHost
	err := tx.Model(&dbHost{}).Where(&dbHost{PublicKey: publicKey(c.HostKey())}).
		Find(&host).Error
	if err != nil {
		return dbContract{}, err
	}

	// Create contract.
	contract := dbContract{
		HostID: host.ID,

		ContractCommon: ContractCommon{
			FCID:        fileContractID(fcid),
			RenewedFrom: fileContractID(renewedFrom),

			TotalCost:      currency(totalCost),
			RevisionNumber: "0",
			StartHeight:    startHeight,
			WindowStart:    c.Revision.WindowStart,
			WindowEnd:      c.Revision.WindowEnd,

			UploadSpending:      zeroCurrency,
			DownloadSpending:    zeroCurrency,
			FundAccountSpending: zeroCurrency,
		},
	}

	// Insert contract.
	err = tx.Create(&contract).Error
	if err != nil {
		return dbContract{}, err
	}
	// Populate host.
	contract.Host = host
	return contract, nil
}

// archiveContracts archives the given contracts and uses the given reason as
// archival reason
//
// NOTE: this function archives the contracts without setting a renewed ID
func archiveContracts(tx *gorm.DB, contracts []dbContract, toArchive map[types.FileContractID]string) error {
	for _, contract := range contracts {
		// sanity check the host is populated
		if contract.Host.ID == 0 {
			return fmt.Errorf("host not populated for contract %v", contract.FCID)
		}

		// create a copy in the archive
		if err := tx.Create(&dbArchivedContract{
			Host:   publicKey(contract.Host.PublicKey),
			Reason: toArchive[types.FileContractID(contract.FCID)],

			ContractCommon: contract.ContractCommon,
		}).Error; err != nil {
			return err
		}

		// remove the contract
		res := tx.Delete(&contract)
		if err := res.Error; err != nil {
			return err
		}
		if res.RowsAffected != 1 {
			return fmt.Errorf("expected to delete 1 row, deleted %d", res.RowsAffected)
		}
	}
	return nil
}

// deleteObject deletes an object from the store.
func deleteObject(tx *gorm.DB, key string) error {
	return tx.Where(&dbObject{ObjectID: key}).Delete(&dbObject{}).Error
}